// Package mqtt implements MQTT v5.0 protocol types, constants, and reason codes.
package mqtt

import "time"

// PacketType represents an MQTT control packet type.
type PacketType byte

const (
	PacketCONNECT     PacketType = 1
	PacketCONNACK     PacketType = 2
	PacketPUBLISH     PacketType = 3
	PacketPUBACK      PacketType = 4
	PacketPUBREC      PacketType = 5
	PacketPUBREL      PacketType = 6
	PacketPUBCOMP     PacketType = 7
	PacketSUBSCRIBE   PacketType = 8
	PacketSUBACK      PacketType = 9
	PacketUNSUBSCRIBE PacketType = 10
	PacketUNSUBACK    PacketType = 11
	PacketPINGREQ     PacketType = 12
	PacketPINGRESP    PacketType = 13
	PacketDISCONNECT  PacketType = 14
	PacketAUTH        PacketType = 15
)

// QoS represents the Quality of Service level.
type QoS byte

const (
	QoS0 QoS = 0 // At most once delivery
	QoS1 QoS = 1 // At least once delivery
	QoS2 QoS = 2 // Exactly once delivery
)

// ReasonCode represents MQTT v5.0 reason codes.
type ReasonCode byte

const (
	ReasonSuccess                       ReasonCode = 0x00
	ReasonNormalDisconnection           ReasonCode = 0x00
	ReasonGrantedQoS0                   ReasonCode = 0x00
	ReasonGrantedQoS1                   ReasonCode = 0x01
	ReasonGrantedQoS2                   ReasonCode = 0x02
	ReasonDisconnectWithWillMessage     ReasonCode = 0x04
	ReasonNoMatchingSubscribers         ReasonCode = 0x10
	ReasonNoSubscriptionFound           ReasonCode = 0x11
	ReasonContinueAuthentication        ReasonCode = 0x18
	ReasonReAuthenticate                ReasonCode = 0x19
	ReasonUnspecifiedError              ReasonCode = 0x80
	ReasonMalformedPacket               ReasonCode = 0x81
	ReasonProtocolError                 ReasonCode = 0x82
	ReasonImplementationSpecificError   ReasonCode = 0x83
	ReasonUnsupportedProtocolVersion    ReasonCode = 0x84
	ReasonClientIdentifierNotValid      ReasonCode = 0x85
	ReasonBadUserNameOrPassword         ReasonCode = 0x86
	ReasonNotAuthorized                 ReasonCode = 0x87
	ReasonServerUnavailable             ReasonCode = 0x88
	ReasonServerBusy                    ReasonCode = 0x89
	ReasonBanned                        ReasonCode = 0x8A
	ReasonServerShuttingDown            ReasonCode = 0x8B
	ReasonBadAuthenticationMethod       ReasonCode = 0x8C
	ReasonKeepAliveTimeout              ReasonCode = 0x8D
	ReasonSessionTakenOver              ReasonCode = 0x8E
	ReasonTopicFilterInvalid            ReasonCode = 0x8F
	ReasonTopicNameInvalid              ReasonCode = 0x90
	ReasonPacketIdentifierInUse         ReasonCode = 0x91
	ReasonPacketIdentifierNotFound      ReasonCode = 0x92
	ReasonReceiveMaximumExceeded        ReasonCode = 0x93
	ReasonTopicAliasInvalid             ReasonCode = 0x94
	ReasonPacketTooLarge                ReasonCode = 0x95
	ReasonMessageRateTooHigh            ReasonCode = 0x96
	ReasonQuotaExceeded                 ReasonCode = 0x97
	ReasonAdministrativeAction          ReasonCode = 0x98
	ReasonPayloadFormatInvalid          ReasonCode = 0x99
	ReasonRetainNotSupported            ReasonCode = 0x9A
	ReasonQoSNotSupported               ReasonCode = 0x9B
	ReasonUseAnotherServer              ReasonCode = 0x9C
	ReasonServerMoved                   ReasonCode = 0x9D
	ReasonSharedSubscriptionsNotSupport ReasonCode = 0x9E
	ReasonConnectionRateExceeded        ReasonCode = 0x9F
	ReasonMaximumConnectTime            ReasonCode = 0xA0
	ReasonSubscriptionIdentifiersNotSup ReasonCode = 0xA1
	ReasonWildcardSubscriptionsNotSupp  ReasonCode = 0xA2
)

// Property identifiers for MQTT v5.0.
const (
	PropPayloadFormatIndicator          = 0x01
	PropMessageExpiryInterval           = 0x02
	PropContentType                     = 0x03
	PropResponseTopic                   = 0x08
	PropCorrelationData                 = 0x09
	PropSubscriptionIdentifier          = 0x0B
	PropSessionExpiryInterval           = 0x11
	PropAssignedClientIdentifier        = 0x12
	PropServerKeepAlive                 = 0x13
	PropAuthenticationMethod            = 0x15
	PropAuthenticationData              = 0x16
	PropRequestProblemInformation       = 0x17
	PropWillDelayInterval               = 0x18
	PropRequestResponseInformation      = 0x19
	PropResponseInformation             = 0x1A
	PropServerReference                 = 0x1C
	PropReasonString                    = 0x1F
	PropReceiveMaximum                  = 0x21
	PropTopicAliasMaximum               = 0x22
	PropTopicAlias                      = 0x23
	PropMaximumQoS                      = 0x24
	PropRetainAvailable                 = 0x25
	PropUserProperty                    = 0x26
	PropMaximumPacketSize               = 0x27
	PropWildcardSubscriptionAvailable   = 0x28
	PropSubscriptionIdentifierAvailable = 0x29
	PropSharedSubscriptionAvailable     = 0x2A
)

// Protocol constants.
const (
	ProtocolName    = "MQTT"
	ProtocolVersion = 5

	DefaultKeepAlive        = 60 * time.Second
	DefaultSessionExpiry    = 0
	DefaultReceiveMaximum   = 65535
	DefaultMaxPacketSize    = 268435455 // 256 MB
	DefaultTopicAliasMaximum = 0

	MaxPacketID = 65535
)

// ConnectFlags represents the Connect Flags byte in a CONNECT packet.
type ConnectFlags struct {
	CleanStart   bool
	WillFlag     bool
	WillQoS      QoS
	WillRetain   bool
	PasswordFlag bool
	UsernameFlag bool
}

// Properties holds MQTT v5.0 properties.
type Properties struct {
	PayloadFormatIndicator          *bool
	MessageExpiryInterval           *uint32
	ContentType                     string
	ResponseTopic                   string
	CorrelationData                 []byte
	SubscriptionIdentifier          []uint32
	SessionExpiryInterval           *uint32
	AssignedClientIdentifier        string
	ServerKeepAlive                 *uint16
	AuthenticationMethod            string
	AuthenticationData              []byte
	RequestProblemInformation       *bool
	WillDelayInterval               *uint32
	RequestResponseInformation      *bool
	ResponseInformation             string
	ServerReference                 string
	ReasonString                    string
	ReceiveMaximum                  *uint16
	TopicAliasMaximum               *uint16
	TopicAlias                      *uint16
	MaximumQoS                      *byte
	RetainAvailable                 *bool
	UserProperties                  []UserProperty
	MaximumPacketSize               *uint32
	WildcardSubscriptionAvailable   *bool
	SubscriptionIdentifierAvailable *bool
	SharedSubscriptionAvailable     *bool
}

// UserProperty is a key-value pair for MQTT v5.0 user properties.
type UserProperty struct {
	Key   string
	Value string
}

// WillMessage represents the Last Will and Testament message.
type WillMessage struct {
	Topic      string
	Payload    []byte
	QoS        QoS
	Retain     bool
	Properties *Properties
}

// Priority levels for message routing (IIoT optimization).
type Priority int

const (
	PriorityNormal  Priority = 0
	PriorityHigh    Priority = 1
	PriorityCritical Priority = 2
)

// Message represents an MQTT message for internal routing.
type Message struct {
	Topic      string
	Payload    []byte
	QoS        QoS
	Retain     bool
	PacketID   uint16
	Properties *Properties
	Priority   Priority
	Expiry     *time.Time
}

// Subscription represents a client subscription.
type Subscription struct {
	TopicFilter       string
	QoS               QoS
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	SubscriptionID    *uint32
}
