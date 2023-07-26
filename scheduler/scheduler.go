package scheduler

type Scheduler interface {
	SelectCandidateNodes()
	Score()
	Pick()
}
