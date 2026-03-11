// Package openrtb defines clean OpenRTB 2.5/2.6 types optimised for CTV app
// video bid requests. All optional fields use pointers or omitempty so the
// JSON wire format contains only populated values — exactly what demand
// partners expect.
package openrtb

// BidRequest — Open-auction CTV app video request.
// No PMP / no PG deals.
type BidRequest struct {
	ID   string `json:"id"`
	Test *int8  `json:"test,omitempty"`
	AT   *int8  `json:"at,omitempty"`
	TMax *int   `json:"tmax,omitempty"`
	Cur  []string `json:"cur,omitempty"`

	Imp    []Impression `json:"imp"`
	App    App          `json:"app"`
	Device Device       `json:"device"`

	User   *User   `json:"user,omitempty"`
	Regs   *Regs   `json:"regs,omitempty"`
	Source *Source  `json:"source,omitempty"`

	BCat  []string `json:"bcat,omitempty"`
	BAdv  []string `json:"badv,omitempty"`
	WSeat []string `json:"wseat,omitempty"`

	Ext map[string]any `json:"ext,omitempty"`
}

type Impression struct {
	ID                string   `json:"id"`
	TagID             string   `json:"tagid,omitempty"`
	DisplayManager    string   `json:"displaymanager,omitempty"`
	DisplayManagerVer string   `json:"displaymanagerver,omitempty"`
	Instl             *int8    `json:"instl,omitempty"`
	Secure            *int8    `json:"secure,omitempty"`
	BidFloor          *float64 `json:"bidfloor,omitempty"`
	BidFloorCur       string   `json:"bidfloorcur,omitempty"`

	Video Video `json:"video"`

	Ext map[string]any `json:"ext,omitempty"`
}

type Video struct {
	MIMEs       []string `json:"mimes"`
	MinDuration *int     `json:"minduration,omitempty"`
	MaxDuration *int     `json:"maxduration,omitempty"`
	Protocols   []int    `json:"protocols,omitempty"`
	W           *int     `json:"w,omitempty"`
	H           *int     `json:"h,omitempty"`
	StartDelay  *int     `json:"startdelay,omitempty"`

	Placement *int  `json:"placement,omitempty"`
	PLCMT     *int  `json:"plcmt,omitempty"`
	Linearity *int8 `json:"linearity,omitempty"`
	Skip      *int8 `json:"skip,omitempty"`
	SkipMin   *int  `json:"skipmin,omitempty"`
	SkipAfter *int  `json:"skipafter,omitempty"`

	PlaybackMethod []int `json:"playbackmethod,omitempty"`
	PlayBackend    *int  `json:"playbackend,omitempty"`
	Delivery       []int `json:"delivery,omitempty"`
	Pos            *int  `json:"pos,omitempty"`
	API            []int `json:"api,omitempty"`

	MaxExtended   *int  `json:"maxextended,omitempty"`
	MinBitrate    *int  `json:"minbitrate,omitempty"`
	MaxBitrate    *int  `json:"maxbitrate,omitempty"`
	BoxingAllowed *int8 `json:"boxingallowed,omitempty"`

	Ext map[string]any `json:"ext,omitempty"`
}

// App is required for app/CTV inventory.
type App struct {
	ID            string `json:"id,omitempty"`
	Name          string `json:"name,omitempty"`
	Bundle        string `json:"bundle,omitempty"`
	StoreURL      string `json:"storeurl,omitempty"`
	Domain        string `json:"domain,omitempty"`
	StoreID       string `json:"storeid,omitempty"`
	Ver           string `json:"ver,omitempty"`
	PrivacyPolicy *int8  `json:"privacypolicy,omitempty"`
	Paid          *int8  `json:"paid,omitempty"`

	Cat        []string `json:"cat,omitempty"`
	SectionCat []string `json:"sectioncat,omitempty"`
	PageCat    []string `json:"pagecat,omitempty"`
	Keywords   string   `json:"keywords,omitempty"`

	Publisher *Publisher `json:"publisher,omitempty"`
	Content   *Content   `json:"content,omitempty"`

	Ext map[string]any `json:"ext,omitempty"`
}

type Publisher struct {
	ID     string   `json:"id,omitempty"`
	Name   string   `json:"name,omitempty"`
	Domain string   `json:"domain,omitempty"`
	Cat    []string `json:"cat,omitempty"`
}

// Content is especially important for CTV signaling.
type Content struct {
	ID                 string         `json:"id,omitempty"`
	Title              string         `json:"title,omitempty"`
	Series             string         `json:"series,omitempty"`
	Season             string         `json:"season,omitempty"`
	Episode            int            `json:"episode,omitempty"`
	Artist             string         `json:"artist,omitempty"`
	Genre              string         `json:"genre,omitempty"`
	Album              string         `json:"album,omitempty"`
	ISRC               string         `json:"isrc,omitempty"`
	Producer           *Producer      `json:"producer,omitempty"`
	URL                string         `json:"url,omitempty"`
	Cat                []string       `json:"cat,omitempty"`
	ProdQ              *int           `json:"prodq,omitempty"`
	Context            *int           `json:"context,omitempty"`
	ContentRating      string         `json:"contentrating,omitempty"`
	UserRating         string         `json:"userrating,omitempty"`
	QAGMediaRating     *int           `json:"qagmediarating,omitempty"`
	Keywords           string         `json:"keywords,omitempty"`
	LiveStream         *int8          `json:"livestream,omitempty"`
	Sourcerelationship *int8          `json:"sourcerelationship,omitempty"`
	Len                *int           `json:"len,omitempty"`
	Language           string         `json:"language,omitempty"`
	Embeddable         *int8          `json:"embeddable,omitempty"`
	Data               []Data         `json:"data,omitempty"`
	Network            *ChannelEntity `json:"network,omitempty"`
	Channel            *ChannelEntity `json:"channel,omitempty"`

	Ext map[string]any `json:"ext,omitempty"`
}

type Producer struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	Domain string `json:"domain,omitempty"`
}

type ChannelEntity struct {
	ID     string         `json:"id,omitempty"`
	Name   string         `json:"name,omitempty"`
	Domain string         `json:"domain,omitempty"`
	Ext    map[string]any `json:"ext,omitempty"`
}

// Device should be CTV-shaped for connected TV inventory.
type Device struct {
	UA         string `json:"ua,omitempty"`
	SUA        any    `json:"sua,omitempty"`
	IP         string `json:"ip,omitempty"`
	IPv6       string `json:"ipv6,omitempty"`
	DNT        *int8  `json:"dnt,omitempty"`
	LMT        *int8  `json:"lmt,omitempty"`
	JS         *int8  `json:"js,omitempty"`
	Language   string `json:"language,omitempty"`

	DeviceType     *int     `json:"devicetype,omitempty"`
	Make           string   `json:"make,omitempty"`
	Model          string   `json:"model,omitempty"`
	OS             string   `json:"os,omitempty"`
	OSV            string   `json:"osv,omitempty"`
	HWV            string   `json:"hwv,omitempty"`
	H              *int     `json:"h,omitempty"`
	W              *int     `json:"w,omitempty"`
	PXRatio        *float64 `json:"pxratio,omitempty"`
	PPI            *int     `json:"ppi,omitempty"`
	IFA            string   `json:"ifa,omitempty"`
	Carrier        string   `json:"carrier,omitempty"`
	ConnectionType *int     `json:"connectiontype,omitempty"`

	Geo *Geo           `json:"geo,omitempty"`
	Ext map[string]any `json:"ext,omitempty"`
}

type Geo struct {
	Lat       *float64 `json:"lat,omitempty"`
	Lon       *float64 `json:"lon,omitempty"`
	Country   string   `json:"country,omitempty"`
	Region    string   `json:"region,omitempty"`
	Metro     string   `json:"metro,omitempty"`
	City      string   `json:"city,omitempty"`
	ZIP       string   `json:"zip,omitempty"`
	Type      *int     `json:"type,omitempty"`
	Accuracy  *int     `json:"accuracy,omitempty"`
	LastFix   *int     `json:"lastfix,omitempty"`
	IPService *int     `json:"ipservice,omitempty"`
}

type User struct {
	ID       string         `json:"id,omitempty"`
	BuyerUID string         `json:"buyeruid,omitempty"`
	YOB      *int           `json:"yob,omitempty"`
	Gender   string         `json:"gender,omitempty"`
	Keywords string         `json:"keywords,omitempty"`
	Data     []Data         `json:"data,omitempty"`
	Ext      map[string]any `json:"ext,omitempty"`
}

type Data struct {
	ID      string    `json:"id,omitempty"`
	Name    string    `json:"name,omitempty"`
	Segment []Segment `json:"segment,omitempty"`
}

type Segment struct {
	ID    string `json:"id,omitempty"`
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

type Regs struct {
	COPPA  *int8          `json:"coppa,omitempty"`
	GPP    string         `json:"gpp,omitempty"`
	GPPSID []int          `json:"gpp_sid,omitempty"`
	Ext    map[string]any `json:"ext,omitempty"`
}

type Source struct {
	FD     *int8          `json:"fd,omitempty"`
	TID    string         `json:"tid,omitempty"`
	PChain string         `json:"pchain,omitempty"`
	SChain *SupplyChain   `json:"schain,omitempty"`
	Ext    map[string]any `json:"ext,omitempty"`
}

type SupplyChain struct {
	Complete int8         `json:"complete"`
	Nodes    []SChainNode `json:"nodes"`
	Ver      string       `json:"ver"`
}

type SChainNode struct {
	ASI    string `json:"asi"`
	SID    string `json:"sid"`
	HP     int8   `json:"hp"`
	RID    string `json:"rid,omitempty"`
	Name   string `json:"name,omitempty"`
	Domain string `json:"domain,omitempty"`
}
