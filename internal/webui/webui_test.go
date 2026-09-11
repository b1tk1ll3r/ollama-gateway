package webui

import (
	"regexp"
	"strings"
	"testing"
)

func appJS(t *testing.T) string {
	t.Helper()
	b, err := embedded.ReadFile("assets/app.js")
	if err != nil {
		t.Fatalf("read embedded app.js: %v", err)
	}
	return string(b)
}

func TestRenderDispatcherFunctionsExist(t *testing.T) {
	js := appJS(t)
	start := strings.Index(js, "function render(){")
	if start < 0 {
		t.Fatal("render dispatcher not found")
	}
	end := strings.Index(js[start:], "}\n")
	if end < 0 {
		t.Fatal("render dispatcher end not found")
	}
	body := js[start : start+end+1]
	calls := regexp.MustCompile(`\b(render[A-Z][A-Za-z0-9_]*)\(`).FindAllStringSubmatch(body, -1)
	if len(calls) == 0 {
		t.Fatal("render dispatcher contains no render function calls")
	}
	seen := map[string]bool{}
	for _, call := range calls {
		name := call[1]
		if seen[name] {
			continue
		}
		seen[name] = true
		if !strings.Contains(js, "function "+name+"(") {
			t.Errorf("render dispatcher calls %s but app.js does not define it", name)
		}
	}
}

func TestPlacementUIHelpersExist(t *testing.T) {
	js := appJS(t)
	for _, name := range []string{
		"placementRule",
		"placementRows",
		"placementCell",
		"placementSourceLabel",
		"renderPlacement",
		"placementCellHTML",
		"placementWorkerCard",
		"loadPlacement",
		"openPlacementDialog",
		"lines",
		"installedForWorker",
		"savePlacementRule",
		"placementExactAction",
	} {
		if !strings.Contains(js, "function "+name+"(") {
			t.Errorf("missing placement UI helper %s", name)
		}
	}
}

func appCSS(t *testing.T) string {
	t.Helper()
	b, err := embedded.ReadFile("assets/app.css")
	if err != nil {
		t.Fatalf("read embedded app.css: %v", err)
	}
	return string(b)
}

func indexHTML(t *testing.T) string {
	t.Helper()
	b, err := embedded.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("read embedded index.html: %v", err)
	}
	return string(b)
}

func TestProfessionalLightThemeIsEmbedded(t *testing.T) {
	css := appCSS(t)
	for _, want := range []string{
		"color-scheme:light",
		"--bg:#eef2f6",
		"--surface:#ffffff",
		"--text:#253345",
		"--font-mono:",
		"Checkpoint 24 — SCMDB-inspired professional light control-plane theme",
		".live-flow-stage,.cluster-map-stage",
	} {
		if !strings.Contains(css, want) {
			t.Errorf("light theme marker %q missing from app.css", want)
		}
	}
	if strings.Contains(indexHTML(t), `name="color-scheme" content="dark light"`) {
		t.Fatal("index still advertises dark color scheme")
	}
	if !strings.Contains(indexHTML(t), `name="color-scheme" content="light"`) {
		t.Fatal("index does not advertise light color scheme")
	}
}

func TestCanvasThemeUsesLightNodeSurfaces(t *testing.T) {
	js := appJS(t)
	for _, want := range []string{
		"rgba(255,255,255,.97)",
		"rgba(37,51,69,.94)",
		"const liveColor=(v,a=1,l=44)",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("light canvas marker %q missing from app.js", want)
		}
	}
	for _, legacy := range []string{
		"rgba(10,18,25,.96)",
		"rgba(11,20,27,.94)",
	} {
		if strings.Contains(js, legacy) {
			t.Errorf("legacy dark canvas color %q still present", legacy)
		}
	}
}

func TestPolicyDialogCloseControlsBypassFormValidation(t *testing.T) {
	html := indexHTML(t)
	if strings.Contains(html, `<form method="dialog" class="dialog-card" id="policy-form">`) {
		t.Fatal("policy dialog still uses method=dialog; required fields can block cancel controls")
	}
	for _, want := range []string{
		`<button class="icon-button" type="button" data-close-dialog="policy-dialog" aria-label="Dialog schließen">×</button>`,
		`<button type="button" class="button" data-close-dialog="policy-dialog">Abbrechen</button>`,
		`<button id="policy-submit" type="submit" class="button primary">Speichern</button>`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("policy dialog control missing or unsafe: %s", want)
		}
	}
	js := appJS(t)
	if !strings.Contains(js, `document.getElementById(b.dataset.closeDialog)?.close()`) {
		t.Fatal("generic data-close-dialog handler missing")
	}
	if strings.Contains(js, `if(e.submitter?.value==='cancel')return;e.preventDefault();`) {
		t.Fatal("policy submit handler still depends on cancel submit semantics")
	}
}

func TestDialogCloseTargetsExist(t *testing.T) {
	html := indexHTML(t)
	dialogIDs := map[string]bool{}
	for _, m := range regexp.MustCompile(`<dialog\s+id="([^"]+)"`).FindAllStringSubmatch(html, -1) {
		dialogIDs[m[1]] = true
	}
	for _, m := range regexp.MustCompile(`data-close-dialog="([^"]+)"`).FindAllStringSubmatch(html, -1) {
		if !dialogIDs[m[1]] {
			t.Errorf("data-close-dialog points to missing dialog %q", m[1])
		}
	}
}

func TestRequiredDialogsHaveValidationIndependentCloseButtons(t *testing.T) {
	html := indexHTML(t)
	for _, dialogID := range []string{"policy-dialog", "pull-dialog"} {
		pattern := regexp.MustCompile(`(?s)<dialog\s+id="` + regexp.QuoteMeta(dialogID) + `".*?</dialog>`)
		block := pattern.FindString(html)
		if block == "" {
			t.Fatalf("dialog %s not found", dialogID)
		}
		if !strings.Contains(block, `required`) {
			t.Fatalf("test assumption failed: dialog %s has no required field", dialogID)
		}
		if !strings.Contains(block, `type="button"`) || !strings.Contains(block, `data-close-dialog="`+dialogID+`"`) {
			t.Errorf("dialog %s lacks a validation-independent close control", dialogID)
		}
	}
}

func TestModelTableExposesDistinctContextWindows(t *testing.T) {
	js := appJS(t)
	for _, want := range []string{"Modell-Max", "Konfig.", "Geladen", "configured_context_length", "loaded_context_length"} {
		if !strings.Contains(js, want) {
			t.Errorf("model context UI marker %q missing", want)
		}
	}
}
