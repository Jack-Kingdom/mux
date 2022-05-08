package mux

type Addr struct {}

func (addr *Addr) Network() string {
	return "mux"
}

func (addr *Addr) String() string {
	return "mux"
}
