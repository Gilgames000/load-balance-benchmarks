package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type Customer struct {
	ArrivalTime time.Time
	WaitingTime time.Duration
	ServiceTime time.Duration
}

func float64ToDuration(seconds float64) time.Duration {
	seconds, milliseconds := math.Modf(seconds)
	milliseconds, microseconds := math.Modf(milliseconds * 1000)
	microseconds, nanoseconds := math.Modf(microseconds * 1000)
	nanoseconds, _ = math.Modf(nanoseconds * 1000)
	t := time.Duration(seconds) * time.Second
	t += time.Duration(milliseconds) * time.Millisecond
	t += time.Duration(microseconds) * time.Microsecond
	t += time.Duration(nanoseconds) * time.Nanosecond

	return t
}

func generateCustomers(lambda float64, customers chan<- Customer, done <-chan struct{}) {
	for {
		t := rand.ExpFloat64() / lambda
		waitingTime := float64ToDuration(t)
		select {
		case <-done:
			return
		case <-time.After(waitingTime):
			customers <- Customer{ArrivalTime: time.Now()}
		}
	}
}

func serveCustomers(mu float64, customers <-chan Customer, out chan<- Customer, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case c := <-customers:
			c.WaitingTime = time.Now().Sub(c.ArrivalTime)
			t := rand.ExpFloat64() / mu
			serviceTime := float64ToDuration(t)
			<-time.After(serviceTime)
			c.ServiceTime = serviceTime
			out <- c
		}
	}
}

func randomFanOut(customers <-chan Customer, servers []chan Customer, d int, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case c := <-customers:
			s := rand.Perm(len(servers))[:d]
			best := s[0]
			for _, i := range s[1:] {
				if len(servers[best]) > len(servers[i]) {
					best = i
				}
			}
			servers[best] <- c
		}
	}
}

func shortestQueueSubsetBalancer(lambda, mu float64, n, d int, t time.Duration) []Customer {
	var result []Customer
	done := make(chan struct{})
	customers := make(chan Customer, 8192)
	servers := make([]chan Customer, n)
	out := make(chan Customer, 2048)

	for i := 0; i < n; i++ {
		servers[i] = make(chan Customer, 2048)
		go serveCustomers(mu, servers[i], out, done)
	}
	go randomFanOut(customers, servers, d, done)
	go generateCustomers(lambda, customers, done)

	time.AfterFunc(t, func() {
		close(done)
	})

	for {
		select {
		case <-done:
			return result
		case c := <-out:
			result = append(result, c)
		}
	}
}

func shortestQueueBalancer(lambda, mu float64, n int, t time.Duration) []Customer {
	return shortestQueueSubsetBalancer(lambda, mu, n, n, t)
}

func randomBalancer(lambda, mu float64, n int, t time.Duration) []Customer {
	return shortestQueueSubsetBalancer(lambda, mu, n, 1, t)
}

func singleBigServer(lambda, mu float64, t time.Duration) []Customer {
	return randomBalancer(lambda, mu, 1, t)
}

func averageWaitingTime(customers []Customer) time.Duration {
	var sum float64

	for _, c := range customers {
		sum += c.WaitingTime.Seconds()
	}

	return float64ToDuration(sum / float64(len(customers)))
}

func main() {
	t := 20 * time.Second // time limit
	//m := math.MaxInt32    // customer limit
	lambda := 0.75
	mu := 1.0
	n := 10 // number of servers

	random := randomBalancer(lambda*float64(n), mu, n, t)
	single := singleBigServer(lambda*float64(n), mu*float64(n), t)
	sq := shortestQueueBalancer(lambda*float64(n), mu, n, t)
	sq2 := shortestQueueSubsetBalancer(lambda*float64(n), mu, n, 2, t)

	fmt.Println("Average waiting time")
	fmt.Printf("Random choice: %v\n", averageWaitingTime(random))
	fmt.Printf("Single big server: %v\n", averageWaitingTime(single))
	fmt.Printf("Shortest queue: %v\n", averageWaitingTime(sq))
	fmt.Printf("Shortest queue subset (d=2): %v\n", averageWaitingTime(sq2))
}
