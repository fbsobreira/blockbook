package trx

// ClientSubscription trx subscription structure
type ClientSubscription struct {
	err chan error
}

// Unsubscribe unsubscribes the notification
func (sub *ClientSubscription) Unsubscribe() {

}
