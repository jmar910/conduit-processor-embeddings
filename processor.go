package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

type llmEmbedProcessor struct {
	sdk.UnimplementedProcessor
	// config llmEmbedConfig
}

//go:generate paramgen -output=llmEmbed_paramgen.go llmEmbedConfig

type llmEmbedConfig struct {
	// Field is the target field that will be set.
	SourceField string `json:"source"`
	// Name is the value of the field to add.
	TargetField string `json:"target"`
}

func (p *llmEmbedProcessor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:        "llmEmbedProcessor",
		Summary:     "Turns some data into llm embeddings",
		Description: "This processor takes some data and turns it into llm embeddings",
		Version:     "v1.0.0",
		Author:      "James Martinez",
		Parameters:  llmEmbedConfig{}.Parameters(),
	}, nil
}

func NewProcessor() sdk.Processor {
	return &llmEmbedProcessor{}
}

func (p *llmEmbedProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	out := make([]sdk.ProcessedRecord, 0, len(records))
	for _, record := range records {
		r, err := p.processRecord(ctx, record)
		if err != nil {
			return append(out, sdk.ErrorRecord{Error: err})
		}

		out = append(out, r)
	}

	return out
}

func (p *llmEmbedProcessor) processRecord(ctx context.Context, record opencdc.Record) (sdk.ProcessedRecord, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://localhost:8091/embedding",
		bytes.NewReader(record.Payload.After.Bytes()),
	)

	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		errClose := resp.Body.Close()
		if errClose != nil {
			sdk.Logger(ctx).
				Err(errClose).
				Msg("failed closing response body (possible resource leak)")
		}
	}()

	ref, err := sdk.NewReferenceResolver(".Payload.After.content")

	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	j, err := json.Marshal(data)

	if err != nil {
		return nil, err
	}

	p.setField(&record, &ref, j)

	return sdk.SingleRecord(record), nil
}

func (p *llmEmbedProcessor) setField(r *opencdc.Record, refRes *sdk.ReferenceResolver, data any) error {
	if refRes == nil {
		return nil
	}

	ref, err := refRes.Resolve(r)
	if err != nil {
		return err
	}

	err = ref.Set(data)
	if err != nil {
		return err
	}

	return nil
}
