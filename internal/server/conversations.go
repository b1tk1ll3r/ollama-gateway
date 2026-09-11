package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/conversation"
)

type responseConversationPlan struct {
	Store        bool
	RequestItems []any
}

// prepareResponseConversation expands previous_response_id into a flattened
// Responses API input history when the optional encrypted conversation store
// is enabled. When disabled the request is left untouched, preserving the
// gateway's content-free default behavior and any upstream-native semantics.
func (s *Server) prepareResponseConversation(body []byte, id auth.Identity) ([]byte, *responseConversationPlan, error) {
	if s.conversations == nil || !s.conversations.Enabled() || len(body) == 0 {
		return body, nil, nil
	}
	var req map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		return nil, nil, fmt.Errorf("decode Responses request: %w", err)
	}
	store := true
	if v, ok := req["store"].(bool); ok && !v {
		store = false
	}
	current, err := responseInputItems(req["input"])
	if err != nil {
		return nil, nil, err
	}
	items := current
	if rawPrev, ok := req["previous_response_id"]; ok {
		prev, ok := rawPrev.(string)
		prev = strings.TrimSpace(prev)
		if !ok || prev == "" {
			return nil, nil, errors.New("previous_response_id must be a non-empty string")
		}
		parent, found, err := s.conversations.Get(prev, id.Tenant, id.Actor())
		if err != nil {
			return nil, nil, fmt.Errorf("load previous response: %w", err)
		}
		if !found {
			return nil, nil, fmt.Errorf("previous_response_id %q was not found for this identity", prev)
		}
		var prior []any
		if err := json.Unmarshal(parent.Context, &prior); err != nil {
			return nil, nil, fmt.Errorf("decode stored conversation context: %w", err)
		}
		items = make([]any, 0, len(prior)+len(current))
		items = append(items, prior...)
		items = append(items, current...)
		req["input"] = items
		delete(req, "previous_response_id")
	}
	out, err := json.Marshal(req)
	if err != nil {
		return nil, nil, fmt.Errorf("encode expanded Responses request: %w", err)
	}
	return out, &responseConversationPlan{Store: store, RequestItems: append([]any(nil), items...)}, nil
}

func responseInputItems(v any) ([]any, error) {
	if v == nil {
		return nil, nil
	}
	switch x := v.(type) {
	case string:
		return []any{map[string]any{"role": "user", "content": x}}, nil
	case []any:
		return append([]any(nil), x...), nil
	case map[string]any:
		return []any{x}, nil
	default:
		return nil, errors.New("Responses input must be a string, object, or array")
	}
}

func (s *Server) persistResponseConversation(plan *responseConversationPlan, captured []byte, truncated bool, id auth.Identity, model string) {
	if plan == nil || !plan.Store || s.conversations == nil || !s.conversations.Enabled() {
		return
	}
	if truncated {
		s.log.Warn("conversation response not stored because capture exceeded limit", "tenant", id.Tenant, "actor", id.Actor(), "model", model)
		return
	}
	responseID, output, err := parseResponsesOutput(captured)
	if err != nil {
		s.log.Warn("conversation response not stored", "tenant", id.Tenant, "actor", id.Actor(), "model", model, "error", err)
		return
	}
	if responseID == "" {
		s.log.Warn("conversation response not stored because response id was absent", "tenant", id.Tenant, "actor", id.Actor(), "model", model)
		return
	}
	ctx := make([]any, 0, len(plan.RequestItems)+len(output))
	ctx = append(ctx, plan.RequestItems...)
	ctx = append(ctx, output...)
	raw, err := json.Marshal(ctx)
	if err != nil {
		s.log.Warn("conversation context encode failed", "response_id", responseID, "error", err)
		return
	}
	if err := s.conversations.Put(conversation.Entry{ID: responseID, Tenant: id.Tenant, Actor: id.Actor(), Model: model, CreatedAt: time.Now().UTC(), Context: raw}); err != nil {
		s.log.Warn("conversation persistence failed", "response_id", responseID, "tenant", id.Tenant, "actor", id.Actor(), "error", err)
	}
}

func parseResponsesOutput(body []byte) (string, []any, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return "", nil, errors.New("empty Responses response")
	}
	if body[0] == '{' {
		var v map[string]any
		if err := json.Unmarshal(body, &v); err != nil {
			return "", nil, err
		}
		return responseIDAndOutput(v)
	}
	var responseID string
	var completedOutput []any
	var doneOutput []any
	for _, rawLine := range bytes.Split(body, []byte{'\n'}) {
		line := bytes.TrimSpace(rawLine)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		var ev map[string]any
		if json.Unmarshal(data, &ev) != nil {
			continue
		}
		typ, _ := ev["type"].(string)
		if resp, ok := ev["response"].(map[string]any); ok {
			id, out, _ := responseIDAndOutput(resp)
			if id != "" {
				responseID = id
			}
			if typ == "response.completed" && out != nil {
				completedOutput = out
			}
		}
		if typ == "response.output_item.done" {
			if item, ok := ev["item"].(map[string]any); ok {
				doneOutput = append(doneOutput, item)
			}
		}
	}
	if responseID == "" {
		return "", nil, errors.New("stream did not contain a response id")
	}
	if completedOutput != nil {
		return responseID, completedOutput, nil
	}
	if len(doneOutput) > 0 {
		return responseID, doneOutput, nil
	}
	return "", nil, errors.New("stream did not contain completed output items")
}

func responseIDAndOutput(v map[string]any) (string, []any, error) {
	id, _ := v["id"].(string)
	out, _ := v["output"].([]any)
	if id == "" {
		return "", nil, errors.New("response id is missing")
	}
	if out == nil {
		return id, nil, errors.New("response output is missing")
	}
	return id, out, nil
}
