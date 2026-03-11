package openrtb

import (
	"encoding/json"

	"github.com/prebid/openrtb/v20/openrtb2"
)

// FromPrebidRequest converts a Prebid openrtb2.BidRequest into the clean
// wire-format BidRequest used for outbound demand partner requests.
func FromPrebidRequest(req *openrtb2.BidRequest) *BidRequest {
	if req == nil {
		return nil
	}

	br := &BidRequest{
		ID:    req.ID,
		Cur:   req.Cur,
		BCat:  req.BCat,
		BAdv:  req.BAdv,
		WSeat: req.WSeat,
		Ext:   rawToMap(req.Ext),
	}

	if req.Test != 0 {
		br.Test = &req.Test
	}
	if req.AT != 0 {
		at := int8(req.AT)
		br.AT = &at
	}
	if req.TMax != 0 {
		tmax := int(req.TMax)
		br.TMax = &tmax
	}

	for _, imp := range req.Imp {
		br.Imp = append(br.Imp, convertImp(imp))
	}

	if req.App != nil {
		br.App = convertApp(req.App)
	}
	if req.Device != nil {
		br.Device = convertDevice(req.Device)
	}
	if req.User != nil {
		br.User = convertUser(req.User)
	}
	if req.Regs != nil {
		br.Regs = convertRegs(req.Regs)
	}
	if req.Source != nil {
		br.Source = convertSource(req.Source)
	}

	return br
}

// ── Impression ───────────────────────────────────────────────────────────────

func convertImp(imp openrtb2.Imp) Impression {
	ci := Impression{
		ID:                imp.ID,
		TagID:             imp.TagID,
		DisplayManager:    imp.DisplayManager,
		DisplayManagerVer: imp.DisplayManagerVer,
		Secure:            imp.Secure,
		BidFloorCur:       imp.BidFloorCur,
		Ext:               rawToMap(imp.Ext),
	}
	if imp.Instl != 0 {
		ci.Instl = &imp.Instl
	}
	if imp.BidFloor != 0 {
		ci.BidFloor = &imp.BidFloor
	}
	if imp.Video != nil {
		ci.Video = convertVideo(imp.Video)
	}
	return ci
}

// ── Video ────────────────────────────────────────────────────────────────────

func convertVideo(v *openrtb2.Video) Video {
	cv := Video{
		MIMEs:         v.MIMEs,
		Skip:          v.Skip,
		BoxingAllowed: v.BoxingAllowed,
		Ext:           rawToMap(v.Ext),
	}

	if v.MinDuration != 0 {
		d := int(v.MinDuration)
		cv.MinDuration = &d
	}
	if v.MaxDuration != 0 {
		d := int(v.MaxDuration)
		cv.MaxDuration = &d
	}
	if len(v.Protocols) > 0 {
		p := make([]int, len(v.Protocols))
		for i, x := range v.Protocols {
			p[i] = int(x)
		}
		cv.Protocols = p
	}
	if v.W != nil {
		w := int(*v.W)
		cv.W = &w
	}
	if v.H != nil {
		h := int(*v.H)
		cv.H = &h
	}
	if v.StartDelay != nil {
		sd := int(*v.StartDelay)
		cv.StartDelay = &sd
	}
	if v.Placement != 0 {
		p := int(v.Placement)
		cv.Placement = &p
	}
	if v.Plcmt != 0 {
		p := int(v.Plcmt)
		cv.PLCMT = &p
	}
	if v.Linearity != 0 {
		l := int8(v.Linearity)
		cv.Linearity = &l
	}
	if v.SkipMin != 0 {
		s := int(v.SkipMin)
		cv.SkipMin = &s
	}
	if v.SkipAfter != 0 {
		s := int(v.SkipAfter)
		cv.SkipAfter = &s
	}
	if len(v.PlaybackMethod) > 0 {
		pm := make([]int, len(v.PlaybackMethod))
		for i, x := range v.PlaybackMethod {
			pm[i] = int(x)
		}
		cv.PlaybackMethod = pm
	}
	if v.PlaybackEnd != 0 {
		pe := int(v.PlaybackEnd)
		cv.PlayBackend = &pe
	}
	if len(v.Delivery) > 0 {
		d := make([]int, len(v.Delivery))
		for i, x := range v.Delivery {
			d[i] = int(x)
		}
		cv.Delivery = d
	}
	if v.Pos != nil {
		p := int(*v.Pos)
		cv.Pos = &p
	}
	if len(v.API) > 0 {
		a := make([]int, len(v.API))
		for i, x := range v.API {
			a[i] = int(x)
		}
		cv.API = a
	}
	if v.MaxExtended != 0 {
		me := int(v.MaxExtended)
		cv.MaxExtended = &me
	}
	if v.MinBitRate != 0 {
		mb := int(v.MinBitRate)
		cv.MinBitrate = &mb
	}
	if v.MaxBitRate != 0 {
		mb := int(v.MaxBitRate)
		cv.MaxBitrate = &mb
	}

	return cv
}

// ── App ──────────────────────────────────────────────────────────────────────

func convertApp(a *openrtb2.App) App {
	ca := App{
		ID:       a.ID,
		Name:     a.Name,
		Bundle:   a.Bundle,
		StoreURL: a.StoreURL,
		Domain:   a.Domain,
		Ver:      a.Ver,
		Cat:      a.Cat,
		SectionCat: a.SectionCat,
		PageCat:  a.PageCat,
		Keywords: a.Keywords,
		Ext:      rawToMap(a.Ext),
	}
	if a.PrivacyPolicy != nil {
		ca.PrivacyPolicy = a.PrivacyPolicy
	}
	if a.Paid != nil {
		ca.Paid = a.Paid
	}
	if a.Publisher != nil {
		ca.Publisher = convertPublisher(a.Publisher)
	}
	if a.Content != nil {
		ca.Content = convertContent(a.Content)
	}
	return ca
}

func convertPublisher(p *openrtb2.Publisher) *Publisher {
	return &Publisher{
		ID:     p.ID,
		Name:   p.Name,
		Domain: p.Domain,
		Cat:    p.Cat,
	}
}

// ── Content ──────────────────────────────────────────────────────────────────

func convertContent(c *openrtb2.Content) *Content {
	ct := &Content{
		ID:            c.ID,
		Title:         c.Title,
		Series:        c.Series,
		Season:        c.Season,
		Artist:        c.Artist,
		Genre:         c.Genre,
		Album:         c.Album,
		ISRC:          c.ISRC,
		URL:           c.URL,
		Cat:           c.Cat,
		ContentRating: c.ContentRating,
		UserRating:    c.UserRating,
		Keywords:      c.Keywords,
		Language:      c.Language,
		LiveStream:    c.LiveStream,
		Embeddable:    c.Embeddable,
		Ext:           rawToMap(c.Ext),
	}
	if c.Episode != 0 {
		ct.Episode = int(c.Episode)
	}
	if c.ProdQ != nil {
		pq := int(*c.ProdQ)
		ct.ProdQ = &pq
	}
	if c.Context != 0 {
		ctx := int(c.Context)
		ct.Context = &ctx
	}
	if c.QAGMediaRating != 0 {
		q := int(c.QAGMediaRating)
		ct.QAGMediaRating = &q
	}
	if c.SourceRelationship != nil {
		ct.Sourcerelationship = c.SourceRelationship
	}
	if c.Len != 0 {
		l := int(c.Len)
		ct.Len = &l
	}
	if c.Producer != nil {
		ct.Producer = &Producer{
			ID:     c.Producer.ID,
			Name:   c.Producer.Name,
			Domain: c.Producer.Domain,
		}
	}
	for _, d := range c.Data {
		cd := Data{ID: d.ID, Name: d.Name}
		for _, s := range d.Segment {
			cd.Segment = append(cd.Segment, Segment{
				ID: s.ID, Name: s.Name, Value: s.Value,
			})
		}
		ct.Data = append(ct.Data, cd)
	}
	if c.Network != nil {
		ct.Network = &ChannelEntity{
			ID: c.Network.ID, Name: c.Network.Name, Domain: c.Network.Domain,
			Ext: rawToMap(c.Network.Ext),
		}
	}
	if c.Channel != nil {
		ct.Channel = &ChannelEntity{
			ID: c.Channel.ID, Name: c.Channel.Name, Domain: c.Channel.Domain,
			Ext: rawToMap(c.Channel.Ext),
		}
	}
	return ct
}

// ── Device ───────────────────────────────────────────────────────────────────

func convertDevice(d *openrtb2.Device) Device {
	cd := Device{
		UA:       d.UA,
		IP:       d.IP,
		IPv6:     d.IPv6,
		DNT:      d.DNT,
		LMT:      d.Lmt,
		Language: d.Language,
		Make:     d.Make,
		Model:    d.Model,
		OS:       d.OS,
		OSV:      d.OSV,
		HWV:      d.HWV,
		IFA:      d.IFA,
		Carrier:  d.Carrier,
		Ext:      rawToMap(d.Ext),
	}
	if d.DeviceType != 0 {
		dt := int(d.DeviceType)
		cd.DeviceType = &dt
	}
	if d.JS != nil {
		cd.JS = d.JS
	}
	if d.H != 0 {
		h := int(d.H)
		cd.H = &h
	}
	if d.W != 0 {
		w := int(d.W)
		cd.W = &w
	}
	if d.PxRatio != 0 {
		cd.PXRatio = &d.PxRatio
	}
	if d.PPI != 0 {
		ppi := int(d.PPI)
		cd.PPI = &ppi
	}
	if d.ConnectionType != nil {
		ct := int(*d.ConnectionType)
		cd.ConnectionType = &ct
	}
	// SUA: pass through the *openrtb2.UserAgent directly — it marshals to
	// the correct JSON representation with browsers, platform, mobile, source.
	if d.SUA != nil {
		cd.SUA = d.SUA
	}
	if d.Geo != nil {
		cd.Geo = convertGeo(d.Geo)
	}
	return cd
}

func convertGeo(g *openrtb2.Geo) *Geo {
	geo := &Geo{
		Country: g.Country,
		Region:  g.Region,
		Metro:   g.Metro,
		City:    g.City,
		ZIP:     g.ZIP,
	}
	if g.Lat != nil {
		geo.Lat = g.Lat
	}
	if g.Lon != nil {
		geo.Lon = g.Lon
	}
	if g.Type != 0 {
		t := int(g.Type)
		geo.Type = &t
	}
	if g.Accuracy != 0 {
		a := int(g.Accuracy)
		geo.Accuracy = &a
	}
	if g.LastFix != 0 {
		lf := int(g.LastFix)
		geo.LastFix = &lf
	}
	if g.IPService != 0 {
		ips := int(g.IPService)
		geo.IPService = &ips
	}
	return geo
}

// ── User ─────────────────────────────────────────────────────────────────────

func convertUser(u *openrtb2.User) *User {
	user := &User{
		ID:       u.ID,
		BuyerUID: u.BuyerUID,
		Gender:   u.Gender,
		Keywords: u.Keywords,
		Ext:      rawToMap(u.Ext),
	}
	if u.Yob != 0 {
		yob := int(u.Yob)
		user.YOB = &yob
	}
	for _, d := range u.Data {
		cd := Data{ID: d.ID, Name: d.Name}
		for _, s := range d.Segment {
			cd.Segment = append(cd.Segment, Segment{
				ID: s.ID, Name: s.Name, Value: s.Value,
			})
		}
		user.Data = append(user.Data, cd)
	}
	return user
}

// ── Regs ─────────────────────────────────────────────────────────────────────

func convertRegs(r *openrtb2.Regs) *Regs {
	regs := &Regs{
		GPP: r.GPP,
	}
	if r.COPPA != 0 {
		regs.COPPA = &r.COPPA
	}
	// Convert GPPSID from []int8 → []int.
	for _, s := range r.GPPSID {
		regs.GPPSID = append(regs.GPPSID, int(s))
	}
	// Move GDPR and US Privacy into ext (OpenRTB 2.5 wire format).
	ext := rawToMap(r.Ext)
	if r.GDPR != nil {
		if ext == nil {
			ext = make(map[string]any)
		}
		ext["gdpr"] = *r.GDPR
	}
	if r.USPrivacy != "" {
		if ext == nil {
			ext = make(map[string]any)
		}
		ext["us_privacy"] = r.USPrivacy
	}
	regs.Ext = ext
	return regs
}

// ── Source ────────────────────────────────────────────────────────────────────

func convertSource(s *openrtb2.Source) *Source {
	src := &Source{
		FD:     s.FD,
		TID:    s.TID,
		PChain: s.PChain,
		Ext:    rawToMap(s.Ext),
	}
	if s.SChain != nil {
		sc := &SupplyChain{
			Complete: s.SChain.Complete,
			Ver:      s.SChain.Ver,
		}
		for _, n := range s.SChain.Nodes {
			node := SChainNode{
				ASI:    n.ASI,
				SID:    n.SID,
				RID:    n.RID,
				Name:   n.Name,
				Domain: n.Domain,
			}
			if n.HP != nil {
				node.HP = *n.HP
			}
			sc.Nodes = append(sc.Nodes, node)
		}
		src.SChain = sc
	}
	return src
}

// ── helpers ──────────────────────────────────────────────────────────────────

// rawToMap unmarshals a json.RawMessage into map[string]any.
// Returns nil for empty/zero-length maps so omitempty elides empty ext objects.
func rawToMap(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	m := make(map[string]any)
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
