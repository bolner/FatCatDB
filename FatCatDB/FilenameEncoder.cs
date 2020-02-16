/*
    Copyright 2020 Tamas Bolner
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
      http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
using System;
using System.Text;
using System.Collections.Generic;

namespace FatCatDB {
    /// <summary>
    /// This class helps in storing arbitrary values in folder or file names
    /// in an OS-agnostic way, while keeping them as readable as possible.
    /// Special characters:
    ///     §: Escape character for 2-char codes. Example: §c = | (see below)
    ///     ~: Placeholder for a space
    ///     °: Placeholder for dot
    ///     ^: "Empty" character, that can be removed. It is inserted before
    ///         upper-case characters to make a difference even on a case-insensitive
    ///         OS, like Windows or MacOS. It is also added at the end, when
    ///         the value is a forbidden word. (see below)
    /// </summary>
    internal class FilenameEncoder {
        private StringBuilder sb = new StringBuilder();

        /// <summary>
        /// List of forbidden file names on Windows (in lower-case)
        /// </summary>
        private HashSet<string> forbidden = new HashSet<string> {
            "con", "prn", "aux", "clock$", "nul",
            "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
            "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
            "$mft", "$mftmirr", "$logfile", "$volume", "$attrdef", "$bitmap", "$boot",
            "$badclus", "$secure", "$upcase", "$extend", "$quota", "$objid", "$reparse"
        };

        /// <summary>
        /// Mapping characters to their §-code
        /// Examples: §c = |, §0 = §, §1 = ~, §f = >
        /// </summary>
        private Dictionary<char, char> charMap = new Dictionary<char, char> {
            {'§', '0'}, {'~', '1'}, {'\0', '2'}, {'\t', '3'}, {'\r', '4'}, {'\n', '5'}, {'\\', '6'},
            {'/', '7'}, {'?', '8'}, {'%', '9'}, {'*', 'a'}, {':', 'b'}, {'|', 'c'}, {'"', 'd'},
            {'<', 'e'}, {'>', 'f'}, {'°', 'g'}, {'^', 'h'}
        };

        /// <summary>
        /// For the inverse character mapping
        /// </summary>
        private Dictionary<char, char> charMapInv = new Dictionary<char, char>();

        /// <summary>
        /// Constructor
        /// </summary>
        internal FilenameEncoder() {
            // Build the inverse map
            foreach(var item in charMap) {
                charMapInv[item.Value] = item.Key;
            }
        }

        /// <summary>
        /// Encodes a value into a representation that is
        /// safe to be used as a file name.
        /// </summary>
        internal string Encode(string value) {
            if (value == null) {
                /*
                    This cannot happen as null values are replaced
                    with a string, specified in the record annotation.
                */
                throw new Exception("FilenameEncoder::Encode(): Received null value");
            }

            if (value.Length < 1) {
                return "^";
            }

            sb.Clear();
            string lowerValue = value.ToLower();
            int length = value.Length;

            if (forbidden.Contains(lowerValue)) {
                sb.Append(value).Append('^');
                return sb.ToString();
            }

            for (int i = 0; i < length; i++) {
                char c = value[i];

                if (charMap.ContainsKey(c)) {
                    sb.Append('§').Append(charMap[c]);
                }
                else if (c != lowerValue[i]) {
                    // Upper-case letter
                    sb.Append('^').Append(c);
                }
                else if (c == ' ') {
                    /*
                        Spaces are encoded, because Windows
                        behaves strangely when a filename starts
                        or ends with spaces (Mostly UI issue).
                    */
                    sb.Append('~');
                }
                else if (c == '.') {
                    /*
                        To have a short encoding for the common
                        dot character.
                    */
                    sb.Append('°');
                } else {
                    sb.Append(c);
                }
            }

            return sb.ToString();
        }

        /// <summary>
        /// Converts back a filename-safe representation
        /// into the original value.
        /// </summary>
        internal string Decode(string value) {
            sb.Clear();
            bool esc = false;

            foreach(char c in value) {
                if (esc) {
                    if (charMapInv.ContainsKey(c)) {
                        sb.Append(charMapInv[c]);
                    } else {
                        sb.Append('§').Append(c);
                    }

                    esc = false;
                }
                else if (c == '^') {
                    continue;
                }
                else if (c == '~') {
                    sb.Append(' ');
                }
                else if (c == '°') {
                    sb.Append('.');
                }
                else if (c == '§') {
                    esc = true;
                    continue;
                }
                else {
                    sb.Append(c);
                }
            }

            return sb.ToString();
        }
    }
}
