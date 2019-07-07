package main

type RemoteSync struct {
	syn *Syncer
}

func newRemoteSync(syn *Syncer) *RemoteSync {
	return &RemoteSync{
		syn: syn,
	}
}
