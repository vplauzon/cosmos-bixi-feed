using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosBixiFeedConsole
{
    internal class BixiEvent
    {
        [Index(0)]
        public DateTime StartDate { get; set; }

        [Index(1)]
        public int StartStationCode { get; set; }

        [Index(2)]
        public DateTime EndDate { get; set; }

        [Index(3)]
        public int EndStationCode { get; set; }

        [Index(4)]
        public int DurationSec { get; set; }

        [Index(5)]
        public int IsMember { get; set; }
    }
}