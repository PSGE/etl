// This files contains schema for Paris TraceRoute tests.
package schema

// TODO(dev): use mixed case Go variable names throughout
type GeolocationIP struct {
	continent_code string
	country_code   string
	country_code3  string
	country_name   string
	region         string
	metro_code     int64
	city           string
	area_code      int64
	postal_code    string
	latitude       float64
	longitude      float64
}

type ParisTracerouteHop struct {
	Protocol      string    `json:"protocal, string"`
	Src_ip        string    `json:"src_ip, string"`
	Src_af        int32     `json:"src_af, int32"`
	Dest_ip       string    `json:"dest_ip, string"`
	Dest_af       int32     `json:"dest_af, int32"`
	Src_hostname  string    `json:"src_hostname, string"`
	Dest_hostname string    `json:"dest_hostname, string"`
	Rtt           []float64 `json:"rtt, []float64"`
}

type MLabConnectionSpecification struct {
	Server_ip      string `json:"server_ip, string"`
	Server_af      int32  `json:"server_af, int32"`
	Client_ip      string `json:"client_ip, string"`
	Client_af      int32  `json:"client_af, int32"`
	Data_direction int32  `json:"data_direction, int32"`
}

type PT struct {
	Test_id              string                      `json:"test_id, string"`
	Project              int32                       `json:"project, int32"`
	Log_time             int64                       `json:"log_time, int64"`
	Connection_spec      MLabConnectionSpecification `json:"connection_spec"`
	Paris_traceroute_hop ParisTracerouteHop          `json:"paris_traceroute_hop"`
	Type                 int32                       `json:"type, int32"`
}
