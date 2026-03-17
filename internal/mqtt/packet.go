// Package mqtt implements MQTT v5.0 packet encoding and decoding.
package mqtt

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// Sentinel errors for packet parsing.
var (
	ErrInvalidPacket        = errors.New("mqtt: invalid packet")
	ErrUnsupportedProtocol  = errors.New("mqtt: unsupported protocol version")
	ErrPacketTooLarge       = errors.New("mqtt: packet too large")
	ErrInvalidQoS           = errors.New("mqtt: invalid QoS level")
	ErrInvalidTopicFilter   = errors.New("mqtt: invalid topic filter")
	ErrInvalidTopicName     = errors.New("mqtt: invalid topic name")
	ErrMalformedLength      = errors.New("mqtt: malformed remaining length")
	ErrProtocol             = errors.New("mqtt: protocol error")
)

// Packet is the interface implemented by all MQTT packets.
type Packet interface {
	Type() PacketType
	Encode(w io.Writer) error
}

// FixedHeader represents the fixed header of an MQTT packet.
type FixedHeader struct {
	PacketType PacketType
	Flags      byte
	Remaining  uint32
}

// ConnectPacket represents an MQTT CONNECT packet.
type ConnectPacket struct {
	FixedHeader
	ProtocolName  string
	ProtocolLevel byte
	ConnectFlags  ConnectFlags
	KeepAlive     uint16
	Properties    *Properties
	ClientID      string
	WillMessage   *WillMessage
	Username      string
	Password      []byte
}

func (p *ConnectPacket) Type() PacketType { return PacketCONNECT }

// ConnackPacket represents an MQTT CONNACK packet.
type ConnackPacket struct {
	FixedHeader
	SessionPresent bool
	ReasonCode     ReasonCode
	Properties     *Properties
}

func (p *ConnackPacket) Type() PacketType { return PacketCONNACK }

// PublishPacket represents an MQTT PUBLISH packet.
type PublishPacket struct {
	FixedHeader
	Dup        bool
	QoS        QoS
	Retain     bool
	TopicName  string
	PacketID   uint16
	Properties *Properties
	Payload    []byte
}

func (p *PublishPacket) Type() PacketType { return PacketPUBLISH }

// PubackPacket represents PUBACK/PUBREC/PUBREL/PUBCOMP.
type PubackPacket struct {
	FixedHeader
	PacketID   uint16
	ReasonCode ReasonCode
	Properties *Properties
}

func (p *PubackPacket) Type() PacketType { return p.FixedHeader.PacketType }

// SubscribePacket represents an MQTT SUBSCRIBE packet.
type SubscribePacket struct {
	FixedHeader
	PacketID      uint16
	Properties    *Properties
	Subscriptions []Subscription
}

func (p *SubscribePacket) Type() PacketType { return PacketSUBSCRIBE }

// SubackPacket represents an MQTT SUBACK packet.
type SubackPacket struct {
	FixedHeader
	PacketID    uint16
	Properties  *Properties
	ReasonCodes []ReasonCode
}

func (p *SubackPacket) Type() PacketType { return PacketSUBACK }

// UnsubscribePacket represents an MQTT UNSUBSCRIBE packet.
type UnsubscribePacket struct {
	FixedHeader
	PacketID     uint16
	Properties   *Properties
	TopicFilters []string
}

func (p *UnsubscribePacket) Type() PacketType { return PacketUNSUBSCRIBE }

// UnsubackPacket represents an MQTT UNSUBACK packet.
type UnsubackPacket struct {
	FixedHeader
	PacketID    uint16
	Properties  *Properties
	ReasonCodes []ReasonCode
}

func (p *UnsubackPacket) Type() PacketType { return PacketUNSUBACK }

// PingreqPacket represents an MQTT PINGREQ packet.
type PingreqPacket struct{ FixedHeader }

func (p *PingreqPacket) Type() PacketType { return PacketPINGREQ }

// PingrespPacket represents an MQTT PINGRESP packet.
type PingrespPacket struct{ FixedHeader }

func (p *PingrespPacket) Type() PacketType { return PacketPINGRESP }

// DisconnectPacket represents an MQTT DISCONNECT packet.
type DisconnectPacket struct {
	FixedHeader
	ReasonCode ReasonCode
	Properties *Properties
}

func (p *DisconnectPacket) Type() PacketType { return PacketDISCONNECT }

// Decoder reads and decodes MQTT packets from a reader.
type Decoder struct {
	r *bufio.Reader
}

// NewDecoder creates a new Decoder for the given reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: bufio.NewReaderSize(r, 4096)}
}

// Decode reads the next packet from the underlying reader.
func (d *Decoder) Decode() (Packet, error) {
	// Read fixed header byte
	b, err := d.r.ReadByte()
	if err != nil {
		return nil, err
	}

	ptype := PacketType(b >> 4)
	flags := b & 0x0F

	// Read remaining length (variable-length encoding)
	remaining, err := decodeVarInt(d.r)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedLength, err)
	}

	if remaining > DefaultMaxPacketSize {
		return nil, ErrPacketTooLarge
	}

	// Read the rest of the packet
	body := make([]byte, remaining)
	if _, err := io.ReadFull(d.r, body); err != nil {
		return nil, fmt.Errorf("%w: reading body: %v", ErrInvalidPacket, err)
	}

	fh := FixedHeader{PacketType: ptype, Flags: flags, Remaining: remaining}
	buf := bytes.NewReader(body)

	switch ptype {
	case PacketCONNECT:
		return decodeConnect(fh, buf)
	case PacketCONNACK:
		return decodeConnack(fh, buf)
	case PacketPUBLISH:
		return decodePublish(fh, buf)
	case PacketPUBACK, PacketPUBREC, PacketPUBREL, PacketPUBCOMP:
		return decodePuback(fh, buf)
	case PacketSUBSCRIBE:
		return decodeSubscribe(fh, buf)
	case PacketSUBACK:
		return decodeSuback(fh, buf)
	case PacketUNSUBSCRIBE:
		return decodeUnsubscribe(fh, buf)
	case PacketUNSUBACK:
		return decodeUnsuback(fh, buf)
	case PacketPINGREQ:
		return &PingreqPacket{FixedHeader: fh}, nil
	case PacketPINGRESP:
		return &PingrespPacket{FixedHeader: fh}, nil
	case PacketDISCONNECT:
		return decodeDisconnect(fh, buf)
	default:
		return nil, fmt.Errorf("%w: unknown packet type %d", ErrInvalidPacket, ptype)
	}
}

// Encode encodes a ConnectPacket to the writer.
func (p *ConnectPacket) Encode(w io.Writer) error {
	var payload bytes.Buffer

	// Protocol name
	writeString(&payload, p.ProtocolName)
	// Protocol level
	payload.WriteByte(p.ProtocolLevel)
	// Connect flags
	var flags byte
	if p.ConnectFlags.CleanStart {
		flags |= 0x02
	}
	if p.ConnectFlags.WillFlag {
		flags |= 0x04
		flags |= byte(p.ConnectFlags.WillQoS) << 3
		if p.ConnectFlags.WillRetain {
			flags |= 0x20
		}
	}
	if p.ConnectFlags.PasswordFlag {
		flags |= 0x40
	}
	if p.ConnectFlags.UsernameFlag {
		flags |= 0x80
	}
	payload.WriteByte(flags)
	// Keep alive
	binary.Write(&payload, binary.BigEndian, p.KeepAlive)
	// Properties
	if err := encodeProperties(&payload, p.Properties); err != nil {
		return err
	}
	// Client ID
	writeString(&payload, p.ClientID)
	// Will
	if p.ConnectFlags.WillFlag && p.WillMessage != nil {
		if err := encodeProperties(&payload, p.WillMessage.Properties); err != nil {
			return err
		}
		writeString(&payload, p.WillMessage.Topic)
		writeBinary(&payload, p.WillMessage.Payload)
	}
	if p.ConnectFlags.UsernameFlag {
		writeString(&payload, p.Username)
	}
	if p.ConnectFlags.PasswordFlag {
		writeBinary(&payload, p.Password)
	}

	return writePacket(w, PacketCONNECT, 0, payload.Bytes())
}

// Encode encodes a ConnackPacket to the writer.
func (p *ConnackPacket) Encode(w io.Writer) error {
	var payload bytes.Buffer
	var ackFlags byte
	if p.SessionPresent {
		ackFlags |= 0x01
	}
	payload.WriteByte(ackFlags)
	payload.WriteByte(byte(p.ReasonCode))
	if err := encodeProperties(&payload, p.Properties); err != nil {
		return err
	}
	return writePacket(w, PacketCONNACK, 0, payload.Bytes())
}

// Encode encodes a PublishPacket to the writer.
func (p *PublishPacket) Encode(w io.Writer) error {
	var payload bytes.Buffer

	writeString(&payload, p.TopicName)
	if p.QoS > QoS0 {
		binary.Write(&payload, binary.BigEndian, p.PacketID)
	}
	if err := encodeProperties(&payload, p.Properties); err != nil {
		return err
	}
	payload.Write(p.Payload)

	var flags byte
	if p.Retain {
		flags |= 0x01
	}
	flags |= byte(p.QoS) << 1
	if p.Dup {
		flags |= 0x08
	}
	return writePacket(w, PacketPUBLISH, flags, payload.Bytes())
}

// Encode encodes a PubackPacket to the writer.
func (p *PubackPacket) Encode(w io.Writer) error {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, p.PacketID)
	if p.ReasonCode != ReasonSuccess || p.Properties != nil {
		payload.WriteByte(byte(p.ReasonCode))
		if err := encodeProperties(&payload, p.Properties); err != nil {
			return err
		}
	}
	var flags byte
	if p.FixedHeader.PacketType == PacketPUBREL {
		flags = 0x02
	}
	return writePacket(w, p.FixedHeader.PacketType, flags, payload.Bytes())
}

// Encode encodes a SubscribePacket to the writer.
func (p *SubscribePacket) Encode(w io.Writer) error {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, p.PacketID)
	if err := encodeProperties(&payload, p.Properties); err != nil {
		return err
	}
	for _, sub := range p.Subscriptions {
		writeString(&payload, sub.TopicFilter)
		var opts byte
		opts |= byte(sub.QoS) & 0x03
		if sub.NoLocal {
			opts |= 0x04
		}
		if sub.RetainAsPublished {
			opts |= 0x08
		}
		opts |= (sub.RetainHandling & 0x03) << 4
		payload.WriteByte(opts)
	}
	return writePacket(w, PacketSUBSCRIBE, 0x02, payload.Bytes())
}

// Encode encodes a SubackPacket to the writer.
func (p *SubackPacket) Encode(w io.Writer) error {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, p.PacketID)
	if err := encodeProperties(&payload, p.Properties); err != nil {
		return err
	}
	for _, rc := range p.ReasonCodes {
		payload.WriteByte(byte(rc))
	}
	return writePacket(w, PacketSUBACK, 0, payload.Bytes())
}

// Encode encodes an UnsubscribePacket to the writer.
func (p *UnsubscribePacket) Encode(w io.Writer) error {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, p.PacketID)
	if err := encodeProperties(&payload, p.Properties); err != nil {
		return err
	}
	for _, tf := range p.TopicFilters {
		writeString(&payload, tf)
	}
	return writePacket(w, PacketUNSUBSCRIBE, 0x02, payload.Bytes())
}

// Encode encodes an UnsubackPacket to the writer.
func (p *UnsubackPacket) Encode(w io.Writer) error {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, p.PacketID)
	if err := encodeProperties(&payload, p.Properties); err != nil {
		return err
	}
	for _, rc := range p.ReasonCodes {
		payload.WriteByte(byte(rc))
	}
	return writePacket(w, PacketUNSUBACK, 0, payload.Bytes())
}

// Encode encodes a PingreqPacket to the writer.
func (p *PingreqPacket) Encode(w io.Writer) error {
	return writePacket(w, PacketPINGREQ, 0, nil)
}

// Encode encodes a PingrespPacket to the writer.
func (p *PingrespPacket) Encode(w io.Writer) error {
	return writePacket(w, PacketPINGRESP, 0, nil)
}

// Encode encodes a DisconnectPacket to the writer.
func (p *DisconnectPacket) Encode(w io.Writer) error {
	var payload bytes.Buffer
	if p.ReasonCode != ReasonNormalDisconnection || p.Properties != nil {
		payload.WriteByte(byte(p.ReasonCode))
		if err := encodeProperties(&payload, p.Properties); err != nil {
			return err
		}
	}
	return writePacket(w, PacketDISCONNECT, 0, payload.Bytes())
}

// ---- Decode helpers ----

func decodeConnect(fh FixedHeader, r *bytes.Reader) (*ConnectPacket, error) {
	p := &ConnectPacket{FixedHeader: fh}
	var err error

	p.ProtocolName, err = readString(r)
	if err != nil {
		return nil, fmt.Errorf("%w: protocol name: %v", ErrInvalidPacket, err)
	}
	if p.ProtocolName != ProtocolName {
		return nil, ErrUnsupportedProtocol
	}

	level, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	p.ProtocolLevel = level
	if p.ProtocolLevel != ProtocolVersion {
		return nil, ErrUnsupportedProtocol
	}

	flags, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	p.ConnectFlags = ConnectFlags{
		CleanStart:   flags&0x02 != 0,
		WillFlag:     flags&0x04 != 0,
		WillQoS:      QoS((flags >> 3) & 0x03),
		WillRetain:   flags&0x20 != 0,
		PasswordFlag: flags&0x40 != 0,
		UsernameFlag: flags&0x80 != 0,
	}

	var keepAlive uint16
	if err := binary.Read(r, binary.BigEndian, &keepAlive); err != nil {
		return nil, err
	}
	p.KeepAlive = keepAlive

	p.Properties, err = decodeProperties(r)
	if err != nil {
		return nil, err
	}

	p.ClientID, err = readString(r)
	if err != nil {
		return nil, err
	}

	if p.ConnectFlags.WillFlag {
		wm := &WillMessage{}
		wm.QoS = p.ConnectFlags.WillQoS
		wm.Retain = p.ConnectFlags.WillRetain
		wm.Properties, err = decodeProperties(r)
		if err != nil {
			return nil, err
		}
		wm.Topic, err = readString(r)
		if err != nil {
			return nil, err
		}
		wm.Payload, err = readBinary(r)
		if err != nil {
			return nil, err
		}
		p.WillMessage = wm
	}

	if p.ConnectFlags.UsernameFlag {
		p.Username, err = readString(r)
		if err != nil {
			return nil, err
		}
	}

	if p.ConnectFlags.PasswordFlag {
		p.Password, err = readBinary(r)
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func decodeConnack(fh FixedHeader, r *bytes.Reader) (*ConnackPacket, error) {
	p := &ConnackPacket{FixedHeader: fh}

	ackFlags, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	p.SessionPresent = ackFlags&0x01 != 0

	rc, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	p.ReasonCode = ReasonCode(rc)

	if r.Len() > 0 {
		p.Properties, err = decodeProperties(r)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func decodePublish(fh FixedHeader, r *bytes.Reader) (*PublishPacket, error) {
	p := &PublishPacket{FixedHeader: fh}
	p.Dup = fh.Flags&0x08 != 0
	p.QoS = QoS((fh.Flags >> 1) & 0x03)
	p.Retain = fh.Flags&0x01 != 0

	if p.QoS > QoS2 {
		return nil, ErrInvalidQoS
	}

	var err error
	p.TopicName, err = readString(r)
	if err != nil {
		return nil, err
	}

	if p.QoS > QoS0 {
		if err := binary.Read(r, binary.BigEndian, &p.PacketID); err != nil {
			return nil, err
		}
	}

	p.Properties, err = decodeProperties(r)
	if err != nil {
		return nil, err
	}

	p.Payload = make([]byte, r.Len())
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return nil, err
	}

	return p, nil
}

func decodePuback(fh FixedHeader, r *bytes.Reader) (*PubackPacket, error) {
	p := &PubackPacket{FixedHeader: fh}

	if err := binary.Read(r, binary.BigEndian, &p.PacketID); err != nil {
		return nil, err
	}

	if r.Len() > 0 {
		rc, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		p.ReasonCode = ReasonCode(rc)

		if r.Len() > 0 {
			p.Properties, err = decodeProperties(r)
			if err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

func decodeSubscribe(fh FixedHeader, r *bytes.Reader) (*SubscribePacket, error) {
	p := &SubscribePacket{FixedHeader: fh}

	if err := binary.Read(r, binary.BigEndian, &p.PacketID); err != nil {
		return nil, err
	}

	var err error
	p.Properties, err = decodeProperties(r)
	if err != nil {
		return nil, err
	}

	for r.Len() > 0 {
		var sub Subscription
		sub.TopicFilter, err = readString(r)
		if err != nil {
			return nil, err
		}
		opts, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		sub.QoS = QoS(opts & 0x03)
		sub.NoLocal = opts&0x04 != 0
		sub.RetainAsPublished = opts&0x08 != 0
		sub.RetainHandling = (opts >> 4) & 0x03
		p.Subscriptions = append(p.Subscriptions, sub)
	}

	return p, nil
}

func decodeSuback(fh FixedHeader, r *bytes.Reader) (*SubackPacket, error) {
	p := &SubackPacket{FixedHeader: fh}

	if err := binary.Read(r, binary.BigEndian, &p.PacketID); err != nil {
		return nil, err
	}

	var err error
	p.Properties, err = decodeProperties(r)
	if err != nil {
		return nil, err
	}

	for r.Len() > 0 {
		rc, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		p.ReasonCodes = append(p.ReasonCodes, ReasonCode(rc))
	}

	return p, nil
}

func decodeUnsubscribe(fh FixedHeader, r *bytes.Reader) (*UnsubscribePacket, error) {
	p := &UnsubscribePacket{FixedHeader: fh}

	if err := binary.Read(r, binary.BigEndian, &p.PacketID); err != nil {
		return nil, err
	}

	var err error
	p.Properties, err = decodeProperties(r)
	if err != nil {
		return nil, err
	}

	for r.Len() > 0 {
		tf, err := readString(r)
		if err != nil {
			return nil, err
		}
		p.TopicFilters = append(p.TopicFilters, tf)
	}

	return p, nil
}

func decodeUnsuback(fh FixedHeader, r *bytes.Reader) (*UnsubackPacket, error) {
	p := &UnsubackPacket{FixedHeader: fh}

	if err := binary.Read(r, binary.BigEndian, &p.PacketID); err != nil {
		return nil, err
	}

	var err error
	p.Properties, err = decodeProperties(r)
	if err != nil {
		return nil, err
	}

	for r.Len() > 0 {
		rc, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		p.ReasonCodes = append(p.ReasonCodes, ReasonCode(rc))
	}

	return p, nil
}

func decodeDisconnect(fh FixedHeader, r *bytes.Reader) (*DisconnectPacket, error) {
	p := &DisconnectPacket{FixedHeader: fh}

	if r.Len() == 0 {
		return p, nil
	}

	rc, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	p.ReasonCode = ReasonCode(rc)

	if r.Len() > 0 {
		p.Properties, err = decodeProperties(r)
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

// ---- Properties encoding/decoding ----

func encodeProperties(w *bytes.Buffer, props *Properties) error {
	if props == nil {
		encodeVarInt(w, 0)
		return nil
	}

	var propBuf bytes.Buffer

	if props.PayloadFormatIndicator != nil {
		propBuf.WriteByte(PropPayloadFormatIndicator)
		if *props.PayloadFormatIndicator {
			propBuf.WriteByte(1)
		} else {
			propBuf.WriteByte(0)
		}
	}
	if props.MessageExpiryInterval != nil {
		propBuf.WriteByte(PropMessageExpiryInterval)
		binary.Write(&propBuf, binary.BigEndian, *props.MessageExpiryInterval)
	}
	if props.ContentType != "" {
		propBuf.WriteByte(PropContentType)
		writeString(&propBuf, props.ContentType)
	}
	if props.ResponseTopic != "" {
		propBuf.WriteByte(PropResponseTopic)
		writeString(&propBuf, props.ResponseTopic)
	}
	if len(props.CorrelationData) > 0 {
		propBuf.WriteByte(PropCorrelationData)
		writeBinary(&propBuf, props.CorrelationData)
	}
	for _, sid := range props.SubscriptionIdentifier {
		propBuf.WriteByte(PropSubscriptionIdentifier)
		encodeVarInt(&propBuf, sid)
	}
	if props.SessionExpiryInterval != nil {
		propBuf.WriteByte(PropSessionExpiryInterval)
		binary.Write(&propBuf, binary.BigEndian, *props.SessionExpiryInterval)
	}
	if props.AssignedClientIdentifier != "" {
		propBuf.WriteByte(PropAssignedClientIdentifier)
		writeString(&propBuf, props.AssignedClientIdentifier)
	}
	if props.ServerKeepAlive != nil {
		propBuf.WriteByte(PropServerKeepAlive)
		binary.Write(&propBuf, binary.BigEndian, *props.ServerKeepAlive)
	}
	if props.AuthenticationMethod != "" {
		propBuf.WriteByte(PropAuthenticationMethod)
		writeString(&propBuf, props.AuthenticationMethod)
	}
	if len(props.AuthenticationData) > 0 {
		propBuf.WriteByte(PropAuthenticationData)
		writeBinary(&propBuf, props.AuthenticationData)
	}
	if props.RequestProblemInformation != nil {
		propBuf.WriteByte(PropRequestProblemInformation)
		if *props.RequestProblemInformation {
			propBuf.WriteByte(1)
		} else {
			propBuf.WriteByte(0)
		}
	}
	if props.WillDelayInterval != nil {
		propBuf.WriteByte(PropWillDelayInterval)
		binary.Write(&propBuf, binary.BigEndian, *props.WillDelayInterval)
	}
	if props.RequestResponseInformation != nil {
		propBuf.WriteByte(PropRequestResponseInformation)
		if *props.RequestResponseInformation {
			propBuf.WriteByte(1)
		} else {
			propBuf.WriteByte(0)
		}
	}
	if props.ResponseInformation != "" {
		propBuf.WriteByte(PropResponseInformation)
		writeString(&propBuf, props.ResponseInformation)
	}
	if props.ServerReference != "" {
		propBuf.WriteByte(PropServerReference)
		writeString(&propBuf, props.ServerReference)
	}
	if props.ReasonString != "" {
		propBuf.WriteByte(PropReasonString)
		writeString(&propBuf, props.ReasonString)
	}
	if props.ReceiveMaximum != nil {
		propBuf.WriteByte(PropReceiveMaximum)
		binary.Write(&propBuf, binary.BigEndian, *props.ReceiveMaximum)
	}
	if props.TopicAliasMaximum != nil {
		propBuf.WriteByte(PropTopicAliasMaximum)
		binary.Write(&propBuf, binary.BigEndian, *props.TopicAliasMaximum)
	}
	if props.TopicAlias != nil {
		propBuf.WriteByte(PropTopicAlias)
		binary.Write(&propBuf, binary.BigEndian, *props.TopicAlias)
	}
	if props.MaximumQoS != nil {
		propBuf.WriteByte(PropMaximumQoS)
		propBuf.WriteByte(*props.MaximumQoS)
	}
	if props.RetainAvailable != nil {
		propBuf.WriteByte(PropRetainAvailable)
		if *props.RetainAvailable {
			propBuf.WriteByte(1)
		} else {
			propBuf.WriteByte(0)
		}
	}
	for _, up := range props.UserProperties {
		propBuf.WriteByte(PropUserProperty)
		writeString(&propBuf, up.Key)
		writeString(&propBuf, up.Value)
	}
	if props.MaximumPacketSize != nil {
		propBuf.WriteByte(PropMaximumPacketSize)
		binary.Write(&propBuf, binary.BigEndian, *props.MaximumPacketSize)
	}
	if props.WildcardSubscriptionAvailable != nil {
		propBuf.WriteByte(PropWildcardSubscriptionAvailable)
		if *props.WildcardSubscriptionAvailable {
			propBuf.WriteByte(1)
		} else {
			propBuf.WriteByte(0)
		}
	}
	if props.SubscriptionIdentifierAvailable != nil {
		propBuf.WriteByte(PropSubscriptionIdentifierAvailable)
		if *props.SubscriptionIdentifierAvailable {
			propBuf.WriteByte(1)
		} else {
			propBuf.WriteByte(0)
		}
	}
	if props.SharedSubscriptionAvailable != nil {
		propBuf.WriteByte(PropSharedSubscriptionAvailable)
		if *props.SharedSubscriptionAvailable {
			propBuf.WriteByte(1)
		} else {
			propBuf.WriteByte(0)
		}
	}

	encodeVarInt(w, uint32(propBuf.Len()))
	w.Write(propBuf.Bytes())
	return nil
}

func decodeProperties(r *bytes.Reader) (*Properties, error) {
	propLen, err := decodeVarInt(r)
	if err != nil {
		return nil, err
	}

	if propLen == 0 {
		return &Properties{}, nil
	}

	propBytes := make([]byte, propLen)
	if _, err := io.ReadFull(r, propBytes); err != nil {
		return nil, err
	}

	props := &Properties{}
	pr := bytes.NewReader(propBytes)

	for pr.Len() > 0 {
		id, err := pr.ReadByte()
		if err != nil {
			return nil, err
		}

		switch id {
		case PropPayloadFormatIndicator:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			b := v == 1
			props.PayloadFormatIndicator = &b

		case PropMessageExpiryInterval:
			var v uint32
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.MessageExpiryInterval = &v

		case PropContentType:
			s, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.ContentType = s

		case PropResponseTopic:
			s, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.ResponseTopic = s

		case PropCorrelationData:
			b, err := readBinary(pr)
			if err != nil {
				return nil, err
			}
			props.CorrelationData = b

		case PropSubscriptionIdentifier:
			v, err := decodeVarInt(pr)
			if err != nil {
				return nil, err
			}
			props.SubscriptionIdentifier = append(props.SubscriptionIdentifier, v)

		case PropSessionExpiryInterval:
			var v uint32
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.SessionExpiryInterval = &v

		case PropAssignedClientIdentifier:
			s, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.AssignedClientIdentifier = s

		case PropServerKeepAlive:
			var v uint16
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.ServerKeepAlive = &v

		case PropAuthenticationMethod:
			s, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.AuthenticationMethod = s

		case PropAuthenticationData:
			b, err := readBinary(pr)
			if err != nil {
				return nil, err
			}
			props.AuthenticationData = b

		case PropRequestProblemInformation:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			b := v == 1
			props.RequestProblemInformation = &b

		case PropWillDelayInterval:
			var v uint32
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.WillDelayInterval = &v

		case PropRequestResponseInformation:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			b := v == 1
			props.RequestResponseInformation = &b

		case PropResponseInformation:
			s, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.ResponseInformation = s

		case PropServerReference:
			s, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.ServerReference = s

		case PropReasonString:
			s, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.ReasonString = s

		case PropReceiveMaximum:
			var v uint16
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.ReceiveMaximum = &v

		case PropTopicAliasMaximum:
			var v uint16
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.TopicAliasMaximum = &v

		case PropTopicAlias:
			var v uint16
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.TopicAlias = &v

		case PropMaximumQoS:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			props.MaximumQoS = &v

		case PropRetainAvailable:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			b := v == 1
			props.RetainAvailable = &b

		case PropUserProperty:
			k, err := readString(pr)
			if err != nil {
				return nil, err
			}
			v, err := readString(pr)
			if err != nil {
				return nil, err
			}
			props.UserProperties = append(props.UserProperties, UserProperty{Key: k, Value: v})

		case PropMaximumPacketSize:
			var v uint32
			if err := binary.Read(pr, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			props.MaximumPacketSize = &v

		case PropWildcardSubscriptionAvailable:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			b := v == 1
			props.WildcardSubscriptionAvailable = &b

		case PropSubscriptionIdentifierAvailable:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			b := v == 1
			props.SubscriptionIdentifierAvailable = &b

		case PropSharedSubscriptionAvailable:
			v, err := pr.ReadByte()
			if err != nil {
				return nil, err
			}
			b := v == 1
			props.SharedSubscriptionAvailable = &b

		default:
			return nil, fmt.Errorf("%w: unknown property 0x%02x", ErrProtocol, id)
		}
	}

	return props, nil
}

// ---- Low-level helpers ----

func writePacket(w io.Writer, ptype PacketType, flags byte, payload []byte) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(ptype)<<4 | flags)
	encodeVarInt(&buf, uint32(len(payload)))
	buf.Write(payload)
	_, err := w.Write(buf.Bytes())
	return err
}

func encodeVarInt(w *bytes.Buffer, v uint32) {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v > 0 {
			b |= 0x80
		}
		w.WriteByte(b)
		if v == 0 {
			break
		}
	}
}

func decodeVarInt(r io.ByteReader) (uint32, error) {
	var result uint32
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 28 {
			return 0, ErrMalformedLength
		}
	}
	return result, nil
}

func writeString(w *bytes.Buffer, s string) {
	if len(s) > math.MaxUint16 {
		s = s[:math.MaxUint16]
	}
	binary.Write(w, binary.BigEndian, uint16(len(s)))
	w.WriteString(s)
}

func readString(r *bytes.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeBinary(w *bytes.Buffer, b []byte) {
	if len(b) > math.MaxUint16 {
		b = b[:math.MaxUint16]
	}
	binary.Write(w, binary.BigEndian, uint16(len(b)))
	w.Write(b)
}

func readBinary(r *bytes.Reader) ([]byte, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
