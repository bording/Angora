namespace RabbitMQClient
{
    static class AmqpConstants
    {
        public const uint FrameHeaderSize = 7;
        public const byte FrameEnd = 0xCE;
        public const byte Reserved = 0x00;

        public static class FrameType
        {
            public const byte Method = 1;
            public const byte Header = 2;
            public const byte Body = 3;
            public const byte Heartbeat = 8;
        }

        public static class ConnectionReplyCode
        {
            public const ushort Success = 200;
            public const ushort ConnectionForced = 320;
            public const ushort InvalidPath = 402;
            public const ushort FrameError = 501;
            public const ushort SyntaxError = 502;
            public const ushort CommandInvalid = 503;
            public const ushort ChannelError = 504;
            public const ushort UnexpectedFrame = 505;
            public const ushort ResourceError = 506;
            public const ushort NotAllowed = 530;
            public const ushort NotImplemented = 540;
            public const ushort InternalError = 541;
        }

        public static class ChannelReplyCode
        {
            public const ushort Success = 200;
            public const ushort AccessRefused = 403;
            public const ushort NotFound = 404;
            public const ushort ResourceLocked = 405;
            public const ushort PreconditionFailed = 406;
        }

        public static class Command
        {
            public static class Connection
            {
                public const ushort ClassId = 10;

                public const ushort Start = 10;
                public const ushort StartOk = 11;
                public const ushort Secure = 20;
                public const ushort SecureOk = 21;
                public const ushort Tune = 30;
                public const ushort TuneOk = 31;
                public const ushort Open = 40;
                public const ushort OpenOk = 41;
                public const ushort Close = 50;
                public const ushort CloseOk = 51;
            }

            public static class Channel
            {
                public const ushort ClassId = 20;

                public const ushort Open = 10;
                public const ushort OpenOk = 11;
                public const ushort Flow = 20;
                public const ushort FlowOk = 21;
                public const ushort Close = 40;
                public const ushort CloseOk = 41;
            }

            public static class Exchange
            {
                public const ushort ClassId = 40;

                public const ushort Declare = 10;
                public const ushort DeclareOk = 11;
                public const ushort Delete = 20;
                public const ushort DeleteOk = 21;
                public const ushort Bind = 30;
                public const ushort BindOk = 31;
                public const ushort Unbind = 40;
                public const ushort UnbindOk = 51;
            }

            public static class Queue
            {
                public const ushort ClassId = 50;

                public const ushort Declare = 10;
                public const ushort DeclareOk = 11;
                public const ushort Bind = 20;
                public const ushort BindOk = 21;
                public const ushort Purge = 30;
                public const ushort PurgeOk = 31;
                public const ushort Delete = 40;
                public const ushort DeleteOk = 41;
                public const ushort Unbind = 50;
                public const ushort UnbindOk = 51;
            }

            public static class Basic
            {
                public const ushort ClassId = 60;

                public const ushort Qos = 10;
                public const ushort QosOk = 11;
                public const ushort Consume = 20;
                public const ushort ConsumeOk = 21;
                public const ushort Cancel = 30;
                public const ushort CancelOk = 31;
                public const ushort Publish = 40;
                public const ushort Return = 50;
                public const ushort Deliver = 60;
                public const ushort Get = 70;
                public const ushort GetOk = 71;
                public const ushort GetEmpty = 72;
                public const ushort Ack = 80;
                public const ushort Reject = 90;
                public const ushort RecoverAsync = 100;
                public const ushort Recover = 110;
                public const ushort RecoverOk = 111;
                public const ushort Nack = 120;
            }
        }
    }
}
