#nullable disable
using System;

namespace BinSync.Crypto.Utilities
{
    public interface IMemoable
    {
        /// <summary>
        /// Produce a copy of this object with its current state.
        /// </summary>
        /// <remarks>
        /// The copy may be used as a base for new state without modifying the original.
        /// </remarks>
        /// <returns>a copy of the object</returns>
        IMemoable Copy();

        /// <summary>
        /// Restore a copied object state into this object.
        /// </summary>
        /// <remarks>
        /// Implementations of this method <em>should</em> try to avoid allocating new memory where possible.
        /// </remarks>
        /// <param name="other">an object previously produced by a call to <see cref="Copy"/> on an object of the same type as this instance.</param>
        void Reset(IMemoable other);
    }
}
