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
using System.Collections.Concurrent;
using Nito.AsyncEx;

namespace FatCatDB {
    /// <summary>
    /// Class for synchronizing access to the packet files
    /// for threads and async tasks.
    /// This is the one and only static class in the project.
    /// </summary>
    internal static class Locking {
        /// <summary>
        /// The size of the collision domain, which determines the number of mutexes.
        /// </summary>
        private const UInt32 BUCKET_COUNT = 4096;

        /// <summary>
        /// A storage for mutex objects, which help in efficient locking.
        /// </summary>
        private static ConcurrentDictionary<UInt32, AsyncLock> mutexes
            = new ConcurrentDictionary<UInt32, AsyncLock>();

        /// <summary>
        /// Hashes a text into a number between 0 and BUCKET_COUNT - 1
        /// </summary>
        /// <param name="text">An arbitrary text</param>
        /// <returns>A number between 0 and BUCKET_COUNT - 1</returns>
        private static UInt32 HashStringToInt(string text) {
            return ((UInt32)text.GetHashCode()) % BUCKET_COUNT;
        }
        
        /// <summary>
        /// Returns a mutex that can be used for both sync and async locking.
        /// Embedded locking, like "lock1{ lock2{ ... } }", is not supported,
        /// and can lead to deadlocks. (Because the hashes don't preserve the
        /// order.)
        /// 
        /// Example for async usage:
        ///     using (await Locking.GetMutex(packet.FullPath).LockAsync()) { ... }
        /// 
        /// Example for synchronous usage:
        ///     using (Locking.GetMutex(packet.FullPath).Lock()) { ... }
        /// 
        /// </summary>
        /// <param name="resourceLocator">Most probably a file path</param>
        internal static AsyncLock GetMutex(string resourceLocator) {
            return mutexes.GetOrAdd(
                HashStringToInt(resourceLocator),
                key => new AsyncLock()
            );
        }
    }
}
