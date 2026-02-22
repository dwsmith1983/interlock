package server

import (
	"github.com/dwsmith1983/interlock/internal/server/handlers"
	"github.com/go-chi/chi/v5"
)

func (s *Server) registerRoutes(r chi.Router) {
	h := handlers.New(s.engine, s.provider, s.registry)

	r.Route("/api", func(r chi.Router) {
		// Health
		r.Get("/health", h.Health)

		// Pipelines
		r.Get("/pipelines", h.ListPipelines)
		r.Post("/pipelines", h.RegisterPipeline)
		r.Get("/pipelines/{pipelineID}", h.GetPipeline)
		r.Delete("/pipelines/{pipelineID}", h.DeletePipeline)

		// Evaluation
		r.Post("/pipelines/{pipelineID}/evaluate", h.EvaluatePipeline)
		r.Get("/pipelines/{pipelineID}/readiness", h.GetReadiness)

		// Traits
		r.Get("/pipelines/{pipelineID}/traits", h.GetTraits)
		r.Get("/pipelines/{pipelineID}/traits/{traitType}", h.GetTrait)
		r.Post("/pipelines/{pipelineID}/traits/{traitType}", h.PushTrait)

		// Runs
		r.Post("/pipelines/{pipelineID}/run", h.RunPipeline)
		r.Get("/pipelines/{pipelineID}/runs", h.ListRuns)
		r.Get("/runs/{runID}", h.GetRun)
		r.Post("/runs/{runID}/complete", h.CompleteRun)

		// Run logs
		r.Get("/pipelines/{pipelineID}/runlogs", h.ListRunLogs)
		r.Get("/pipelines/{pipelineID}/runlogs/{date}", h.GetRunLog)

		// Events
		r.Get("/pipelines/{pipelineID}/events", h.ListEvents)

		// Reruns
		r.Post("/pipelines/{pipelineID}/rerun", h.RequestRerun)
		r.Get("/pipelines/{pipelineID}/reruns", h.ListReruns)
		r.Get("/reruns", h.ListAllReruns)
		r.Post("/reruns/{rerunID}/complete", h.CompleteRerun)
	})
}
