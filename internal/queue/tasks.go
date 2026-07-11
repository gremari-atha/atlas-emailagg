package queue

const (
	// TypeEmailFetch is the task type for triggering email fetching/syncing for a mailbox.
	TypeEmailFetch = "email:fetch"

	// TypeEmailProcess is the task type for parsing and processing raw email bodies.
	TypeEmailProcess = "email:process"

	// TypeEmailDisconnect is the task type for disconnecting and cleaning up credentials for an email account.
	TypeEmailDisconnect = "email:disconnect"
)
