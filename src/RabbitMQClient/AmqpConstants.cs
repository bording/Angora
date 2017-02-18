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
            public const ushort ContentTooLarge = 311;
            public const ushort NoConsumers = 313;
            public const ushort AccessRefused = 403;
            public const ushort NotFound = 404;
            public const ushort ResourceLocked = 405;
            public const ushort PreconditionFailed = 406;
        }

        public static class Class
        {
            public const ushort Connection = 10;
            public const ushort Channel = 20;
            public const ushort Exchange = 40;
            public const ushort Queue = 50;
            public const ushort Basic = 60;
        }

        public static class Method
        {
            public static class Connection
            {
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
                public const ushort Open = 10;
                public const ushort OpenOk = 11;
                public const ushort Flow = 20;
                public const ushort FlowOk = 21;
                public const ushort Close = 40;
                public const ushort CloseOk = 41;
            }

            public static class Exchange
            {
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

        public static class Command
        {
            public static class Connection
            {
                public const uint Start = Class.Connection << 16 | Method.Connection.Start;
                public const uint StartOk = Class.Connection << 16 | Method.Connection.StartOk;
                public const uint Secure = Class.Connection << 16 | Method.Connection.Secure;
                public const uint SecureOk = Class.Connection << 16 | Method.Connection.SecureOk;
                public const uint Tune = Class.Connection << 16 | Method.Connection.Tune;
                public const uint TuneOk = Class.Connection << 16 | Method.Connection.TuneOk;
                public const uint Open = Class.Connection << 16 | Method.Connection.Open;
                public const uint OpenOk = Class.Connection << 16 | Method.Connection.OpenOk;
                public const uint Close = Class.Connection << 16 | Method.Connection.Close;
                public const uint CloseOk = Class.Connection << 16 | Method.Connection.CloseOk;
            }

            public static class Channel
            {
                public const uint Open = Class.Channel << 16 | Method.Channel.Open;
                public const uint OpenOk = Class.Channel << 16 | Method.Channel.OpenOk;
                public const uint Flow = Class.Channel << 16 | Method.Channel.Flow;
                public const uint FlowOk = Class.Channel << 16 | Method.Channel.FlowOk;
                public const uint Close = Class.Channel << 16 | Method.Channel.Close;
                public const uint CloseOk = Class.Channel << 16 | Method.Channel.CloseOk;
            }

            public static class Exchange
            {
                public const uint Declare = Class.Exchange << 16 | Method.Exchange.Declare;
                public const uint DeclareOk = Class.Exchange << 16 | Method.Exchange.DeclareOk;
                public const uint Delete = Class.Exchange << 16 | Method.Exchange.Delete;
                public const uint DeleteOk = Class.Exchange << 16 | Method.Exchange.DeleteOk;
                public const uint Bind = Class.Exchange << 16 | Method.Exchange.Bind;
                public const uint BindOk = Class.Exchange << 16 | Method.Exchange.BindOk;
                public const uint Unbind = Class.Exchange << 16 | Method.Exchange.Unbind;
                public const uint UnbindOk = Class.Exchange << 16 | Method.Exchange.UnbindOk;
            }

            public static class Queue
            {
                public const uint Declare = Class.Queue << 16 | Method.Queue.Declare;
                public const uint DeclareOk = Class.Queue << 16 | Method.Queue.DeclareOk;
                public const uint Bind = Class.Queue << 16 | Method.Queue.Bind;
                public const uint BindOk = Class.Queue << 16 | Method.Queue.BindOk;
                public const uint Purge = Class.Queue << 16 | Method.Queue.Purge;
                public const uint PurgeOk = Class.Queue << 16 | Method.Queue.PurgeOk;
                public const uint Delete = Class.Queue << 16 | Method.Queue.Delete;
                public const uint DeleteOk = Class.Queue << 16 | Method.Queue.DeleteOk;
                public const uint Unbind = Class.Queue << 16 | Method.Queue.Unbind;
                public const uint UnbindOk = Class.Queue << 16 | Method.Queue.UnbindOk;
            }

            public static class Basic
            {
                public const uint Qos = Class.Basic << 16 | Method.Basic.Qos;
                public const uint QosOk = Class.Basic << 16 | Method.Basic.QosOk;
                public const uint Consume = Class.Basic << 16 | Method.Basic.Consume;
                public const uint ConsumeOk = Class.Basic << 16 | Method.Basic.ConsumeOk;
                public const uint Cancel = Class.Basic << 16 | Method.Basic.Cancel;
                public const uint CancelOk = Class.Basic << 16 | Method.Basic.CancelOk;
                public const uint Publish = Class.Basic << 16 | Method.Basic.Publish;
                public const uint Return = Class.Basic << 16 | Method.Basic.Return;
                public const uint Deliver = Class.Basic << 16 | Method.Basic.Deliver;
                public const uint Get = Class.Basic << 16 | Method.Basic.Get;
                public const uint GetOk = Class.Basic << 16 | Method.Basic.GetOk;
                public const uint GetEmpty = Class.Basic << 16 | Method.Basic.GetEmpty;
                public const uint Ack = Class.Basic << 16 | Method.Basic.Ack;
                public const uint Reject = Class.Basic << 16 | Method.Basic.Reject;
                public const uint RecoverAsync = Class.Basic << 16 | Method.Basic.RecoverAsync;
                public const uint Recover = Class.Basic << 16 | Method.Basic.Recover;
                public const uint RecoverOk = Class.Basic << 16 | Method.Basic.RecoverOk;
                public const uint Nack = Class.Basic << 16 | Method.Basic.Nack;
            }
        }
    }
}
