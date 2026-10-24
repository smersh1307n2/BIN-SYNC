#nullable disable
namespace BinSync.Crypto.Utilities
{
    public interface IMemoable
    {
        IMemoable Copy();
        void Reset(IMemoable other);
    }
}
