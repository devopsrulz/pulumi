// Copyright 2017, Pulumi Corporation.  All rights reserved.

package engine

import (
	"bytes"
	"regexp"
	"time"

	"github.com/pulumi/pulumi/pkg/diag"
	"github.com/pulumi/pulumi/pkg/diag/colors"
	"github.com/pulumi/pulumi/pkg/resource"
	"github.com/pulumi/pulumi/pkg/resource/config"
	"github.com/pulumi/pulumi/pkg/resource/deploy"
	"github.com/pulumi/pulumi/pkg/tokens"
	"github.com/pulumi/pulumi/pkg/util/contract"
)

// Event represents an event generated by the engine during an operation. The underlying
// type for the `Payload` field will differ depending on the value of the `Type` field
type Event struct {
	Type    EventType
	Payload interface{}
}

// EventType is the kind of event being emitted.
type EventType string

const (
	CancelEvent             EventType = "cancel"
	StdoutColorEvent        EventType = "stdoutcolor"
	DiagEvent               EventType = "diag"
	PreludeEvent            EventType = "prelude"
	SummaryEvent            EventType = "summary"
	ResourcePreEvent        EventType = "resourcepre"
	ResourceOutputsEvent    EventType = "resourceoutputs"
	ResourceOperationFailed EventType = "resourceoperationfailed"
)

func cancelEvent() Event {
	return Event{Type: CancelEvent}
}

// DiagEventPayload is the payload for an event with type `diag`
type DiagEventPayload struct {
	Message  string
	Color    colors.Colorization
	Severity diag.Severity
}

type StdoutEventPayload struct {
	Message string
	Color   colors.Colorization
}

type PreludeEventPayload struct {
	IsPreview bool              // true if this prelude is for a plan operation
	Config    map[string]string // the keys and values for config. For encrypted config, the values may be blinded
}

type SummaryEventPayload struct {
	IsPreview       bool            // true if this summary is for a plan operation
	MaybeCorrupt    bool            // true if one or more resources may be corrupt
	Duration        time.Duration   // the duration of the entire update operation (zero values for previews)
	ResourceChanges ResourceChanges // count of changed resources, useful for reporting
}

type ResourceOperationFailedPayload struct {
	Metadata StepEventMetdata
	Status   resource.Status
	Steps    int
}

type ResourceOutputsEventPayload struct {
	Metadata StepEventMetdata
	Indent   int
	Text     string
}

type ResourcePreEventPayload struct {
	Metadata StepEventMetdata
	Indent   int
	Summary  string
	Details  string
}

type StepEventMetdata struct {
	Op      deploy.StepOp           // the operation performed by this step.
	URN     resource.URN            // the resource URN (for before and after).
	Type    tokens.Type             // the type affected by this step.
	Old     *StepEventStateMetadata // the state of the resource before performing this step.
	New     *StepEventStateMetadata // the state of the resource after performing this step.
	Res     *StepEventStateMetadata // the latest state for the resource that is known (worst case, old).
	Logical bool                    // true if this step represents a logical operation in the program.
}

type StepEventStateMetadata struct {
	Type    tokens.Type  // the resource's type.
	URN     resource.URN // the resource's object urn, a human-friendly, unique name for the resource.
	Custom  bool         // true if the resource is custom, managed by a plugin.
	Delete  bool         // true if this resource is pending deletion due to a replacement.
	ID      resource.ID  // the resource's unique ID, assigned by the resource provider (or blank if none/uncreated).
	Parent  resource.URN // an optional parent URN that this resource belongs to.
	Protect bool         // true to "protect" this resource (protected resources cannot be deleted).
}

func makeEventEmitter(events chan<- Event, update Update) eventEmitter {
	var f filter = &nopFilter{}

	target := update.GetTarget()

	if target.Config.HasSecureValue() {
		var b bytes.Buffer
		for _, v := range target.Config {
			if !v.Secure() {
				continue
			}

			if b.Len() > 0 {
				b.WriteRune('|')
			}

			secret, err := v.Value(target.Decrypter)
			contract.AssertNoError(err)

			b.WriteString(regexp.QuoteMeta(secret))
		}

		f = &regexFilter{re: regexp.MustCompile(b.String())}
	}

	return eventEmitter{
		Chan:   events,
		Filter: f,
	}
}

type eventEmitter struct {
	Chan   chan<- Event
	Filter filter
}

func makeStepEventMetadata(step deploy.Step) StepEventMetdata {
	return StepEventMetdata{
		Op:      step.Op(),
		URN:     step.URN(),
		Type:    step.Type(),
		Old:     makeStepEventStateMetadata(step.Old()),
		New:     makeStepEventStateMetadata(step.New()),
		Res:     makeStepEventStateMetadata(step.Res()),
		Logical: step.Logical(),
	}
}

func makeStepEventStateMetadata(state *resource.State) *StepEventStateMetadata {
	if state == nil {
		return nil
	}

	return &StepEventStateMetadata{
		Type:    state.Type,
		URN:     state.URN,
		Custom:  state.Custom,
		Delete:  state.Delete,
		ID:      state.ID,
		Parent:  state.Parent,
		Protect: state.Protect,
	}
}

type filter interface {
	Filter(s string) string
}

type nopFilter struct {
}

func (f *nopFilter) Filter(s string) string {
	return s
}

type regexFilter struct {
	re *regexp.Regexp
}

func (f *regexFilter) Filter(s string) string {
	return f.re.ReplaceAllLiteralString(s, "[secret]")
}

func (e *eventEmitter) resourceOperationFailedEvent(step deploy.Step, status resource.Status, steps int) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: ResourceOperationFailed,
		Payload: ResourceOperationFailedPayload{
			Metadata: makeStepEventMetadata(step),
			Status:   status,
			Steps:    steps,
		},
	}
}

func (e *eventEmitter) resourceOutputsEvent(step deploy.Step, indent int, text string) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: ResourceOutputsEvent,
		Payload: ResourceOutputsEventPayload{
			Metadata: makeStepEventMetadata(step),
			Indent:   indent,
			Text:     e.Filter.Filter(text),
		},
	}
}

func (e *eventEmitter) resourcePreEvent(step deploy.Step, indent int, summary string, details string) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: ResourcePreEvent,
		Payload: ResourcePreEventPayload{
			Metadata: makeStepEventMetadata(step),
			Indent:   indent,
			Summary:  e.Filter.Filter(summary),
			Details:  e.Filter.Filter(details),
		},
	}
}

func (e *eventEmitter) preludeEvent(isPreview bool, cfg config.Map) {
	contract.Requiref(e != nil, "e", "!= nil")

	configStringMap := make(map[string]string, len(cfg))
	for k, v := range cfg {
		keyString := k.String()
		valueString, err := v.Value(config.NewBlindingDecrypter())
		contract.AssertNoError(err)
		configStringMap[keyString] = valueString
	}

	e.Chan <- Event{
		Type: PreludeEvent,
		Payload: PreludeEventPayload{
			IsPreview: isPreview,
			Config:    configStringMap,
		},
	}
}

func (e *eventEmitter) previewSummaryEvent(resourceChanges ResourceChanges) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: SummaryEvent,
		Payload: SummaryEventPayload{
			IsPreview:       true,
			MaybeCorrupt:    false,
			Duration:        0,
			ResourceChanges: resourceChanges,
		},
	}
}

func (e *eventEmitter) updateSummaryEvent(maybeCorrupt bool,
	duration time.Duration, resourceChanges ResourceChanges) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: SummaryEvent,
		Payload: SummaryEventPayload{
			IsPreview:       false,
			MaybeCorrupt:    maybeCorrupt,
			Duration:        duration,
			ResourceChanges: resourceChanges,
		},
	}
}

func (e *eventEmitter) diagDebugEvent(msg string) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: DiagEvent,
		Payload: DiagEventPayload{
			Message:  e.Filter.Filter(msg),
			Color:    colors.Raw,
			Severity: diag.Debug,
		},
	}
}

func (e *eventEmitter) diagInfoEvent(msg string) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: DiagEvent,
		Payload: DiagEventPayload{
			Message:  e.Filter.Filter(msg),
			Color:    colors.Raw,
			Severity: diag.Info,
		},
	}
}

func (e *eventEmitter) diagInfoerrEvent(msg string) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: DiagEvent,
		Payload: DiagEventPayload{
			Message:  e.Filter.Filter(msg),
			Color:    colors.Raw,
			Severity: diag.Infoerr,
		},
	}
}

func (e *eventEmitter) diagErrorEvent(msg string) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: DiagEvent,
		Payload: DiagEventPayload{
			Message:  e.Filter.Filter(msg),
			Color:    colors.Raw,
			Severity: diag.Error,
		},
	}
}

func (e *eventEmitter) diagWarningEvent(msg string) {
	contract.Requiref(e != nil, "e", "!= nil")

	e.Chan <- Event{
		Type: DiagEvent,
		Payload: DiagEventPayload{
			Message:  e.Filter.Filter(msg),
			Color:    colors.Raw,
			Severity: diag.Warning,
		},
	}
}
