using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQClient
{
    public static class AmqpConstants
    {
        public const byte FrameEnd = 0xCE;

        public static class FrameType
        {
            public const byte Method = 1;
            public const byte Header = 2;
            public const byte Body = 3;
            public const byte Heartbeat = 8;
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
        }
    }
}
