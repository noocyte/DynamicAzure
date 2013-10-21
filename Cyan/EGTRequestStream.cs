using System.IO;
using System.Text;

namespace Cyan
{
    class EGTRequestStream : MemoryStream
    {
        public void WriteBoundary(string boundary)
        {
            WriteLine("--{0}", boundary);
        }

        public void WriteEndBoundary(string boundary)
        {
            WriteLine("--{0}--", boundary);
        }

        public void WriteHeader(string name, string value)
        {
            WriteLine("{0}: {1}", name, value);
        }

        public void Write(string text)
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            Write(bytes, 0, bytes.Length);
        }

        public void WriteLine()
        {
            Write("\n");
        }

        public void WriteLine(string format, params string[] args)
        {
            Write(string.Format(format + "\n", args));
        }
    }
}
