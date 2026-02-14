using System.Net;
using System.Text;

namespace PgBouncer.Tests.Protocol;

/// <summary>
/// Helper methods for creating PostgreSQL protocol messages for testing.
/// </summary>
public static class PgProtocolHelpers
{
    /// <summary>
    /// Creates a PostgreSQL StartupMessage for protocol version 3.0.
    /// </summary>
    public static byte[] CreateStartupMessage(string user, string database, string applicationName)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Calculate length: 4 bytes for length field + protocol version + parameters
        int length = 4 + 4;

        // Add parameter lengths
        var userBytes = Encoding.ASCII.GetBytes(user);
        var databaseBytes = Encoding.ASCII.GetBytes(database);
        var appBytes = Encoding.ASCII.GetBytes(applicationName);

        length += 5 + 1 + userBytes.Length + 1;  // "user" key + value + null terminators
        length += 9 + 1 + databaseBytes.Length + 1; // "database" key + value + null terminators
        length += 16 + 1 + appBytes.Length + 1;       // "application_name" key + value + null terminators
        length += 1; // Final null terminator

        // Write length
        writer.Write(IPAddress.HostToNetworkOrder(length));

        // Write protocol version 3.0
        writer.Write(IPAddress.HostToNetworkOrder(0x00030000));

        // Write user parameter
        WritePgString(writer, "user");
        WritePgString(writer, user);

        // Write database parameter
        WritePgString(writer, "database");
        WritePgString(writer, database);

        // Write application_name parameter
        WritePgString(writer, "application_name");
        WritePgString(writer, applicationName);

        // Final terminator
        writer.Write((byte)0);

        return ms.ToArray();
    }

    /// <summary>
    /// Creates a PostgreSQL SSLRequest message.
    /// </summary>
    public static byte[] CreateSSLRequest()
    {
        return new byte[]
        {
            0, 0, 0, 8,             // Length: 8 bytes
            0x04, 0xD2, 0x16, 0x2F   // SSLRequest code: 80877103
        };
    }

    /// <summary>
    /// Creates a PostgreSQL Query message.
    /// </summary>
    public static byte[] CreateQueryMessage(string query)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        var queryBytes = Encoding.UTF8.GetBytes(query);

        // Length: 4 bytes for length field + query string + null terminator
        int length = 4 + queryBytes.Length + 1;

        // Write message type 'Q'
        writer.Write((byte)'Q');

        // Write length
        writer.Write(IPAddress.HostToNetworkOrder(length));

        // Write query string
        writer.Write(queryBytes);

        // Write null terminator
        writer.Write((byte)0);

        return ms.ToArray();
    }

    /// <summary>
    /// Creates a PostgreSQL DataRow message with the specified data.
    /// </summary>
    public static byte[] CreateDataRow(params string?[][] values)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Message type 'D'
        writer.Write((byte)'D');

        // Placeholder for length
        var lengthPosition = ms.Position;
        writer.Write(0);

        // Number of columns
        writer.Write(IPAddress.HostToNetworkOrder((short)values.Length));

        // Write each column value
        foreach (var column in values)
        {
            if (column.Length == 0 || column[0] == null)
            {
                // NULL value: write -1 as length
                writer.Write(IPAddress.HostToNetworkOrder(-1));
            }
            else
            {
                var valueBytes = Encoding.UTF8.GetBytes(column[0]!);
                writer.Write(IPAddress.HostToNetworkOrder(valueBytes.Length));
                writer.Write(valueBytes);
            }
        }

        // Write length
        ms.Position = lengthPosition;
        var length = (int)(ms.Length - lengthPosition);
        writer.Write(IPAddress.HostToNetworkOrder(length));

        return ms.ToArray();
    }

    /// <summary>
    /// Creates a PostgreSQL CommandComplete message.
    /// </summary>
    public static byte[] CreateCommandComplete(string tag)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        var tagBytes = Encoding.UTF8.GetBytes(tag);

        // Message type 'C'
        writer.Write((byte)'C');

        // Length: 4 bytes + tag + null terminator
        int length = 4 + tagBytes.Length + 1;
        writer.Write(IPAddress.HostToNetworkOrder(length));

        // Write tag string
        writer.Write(tagBytes);

        // Write null terminator
        writer.Write((byte)0);

        return ms.ToArray();
    }

    /// <summary>
    /// Creates a PostgreSQL ReadyForQuery message.
    /// </summary>
    public static byte[] CreateReadyForQuery(char transactionStatus = 'I')
    {
        return new byte[]
        {
            (byte)'Z',             // Message type
            0, 0, 0, 5,           // Length: 5 bytes
            (byte)transactionStatus // Transaction status: 'I' = Idle, 'T' = In Transaction, 'E' = Error
        };
    }

    /// <summary>
    /// Creates a PostgreSQL ErrorResponse message.
    /// </summary>
    public static byte[] CreateErrorResponse(string message)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        var messageBytes = Encoding.UTF8.GetBytes(message);

        // Message type 'E'
        writer.Write((byte)'E');

        // Placeholder for length
        var lengthPosition = ms.Position;
        writer.Write(0);

        // Error fields: 'M' = Message
        writer.Write((byte)'M');
        writer.Write(IPAddress.HostToNetworkOrder(messageBytes.Length + 1));
        writer.Write(messageBytes);
        writer.Write((byte)0);

        // Severity: 'S' = ERROR
        writer.Write((byte)'S');
        writer.Write(IPAddress.HostToNetworkOrder(6)); // "ERROR" length + 1
        writer.Write(Encoding.ASCII.GetBytes("ERROR"));
        writer.Write((byte)0);

        // Terminator
        writer.Write((byte)0);

        // Write length
        ms.Position = lengthPosition;
        var length = (int)(ms.Length - lengthPosition);
        writer.Write(IPAddress.HostToNetworkOrder(length));

        return ms.ToArray();
    }

    /// <summary>
    /// Creates a PostgreSQL AuthenticationOk message.
    /// </summary>
    public static byte[] CreateAuthenticationOk()
    {
        return new byte[]
        {
            (byte)'R',        // Message type
            0, 0, 0, 8,      // Length: 8 bytes
            0, 0, 0, 0       // AuthenticationOk code: 0
        };
    }

    /// <summary>
    /// Creates a PostgreSQL RowDescription message.
    /// </summary>
    public static byte[] CreateRowDescription(params (string Name, int TableOid, short ColumnNumber, int TypeOid, short TypeLen, int TypeMod, int FormatCode)[] columns)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Message type 'T'
        writer.Write((byte)'T');

        // Placeholder for length
        var lengthPosition = ms.Position;
        writer.Write(0);

        // Number of fields
        writer.Write(IPAddress.HostToNetworkOrder((short)columns.Length));

        // Write each field description
        foreach (var col in columns)
        {
            var nameBytes = Encoding.UTF8.GetBytes(col.Name);

            // Field name
            writer.Write(nameBytes);
            writer.Write((byte)0);

            // Table OID
            writer.Write(IPAddress.HostToNetworkOrder(col.TableOid));

            // Column number (attribute number of column)
            writer.Write(IPAddress.HostToNetworkOrder(col.ColumnNumber));

            // Type OID
            writer.Write(IPAddress.HostToNetworkOrder(col.TypeOid));

            // Type length (negative = variable length)
            writer.Write(IPAddress.HostToNetworkOrder(col.TypeLen));

            // Type modifier
            writer.Write(IPAddress.HostToNetworkOrder(col.TypeMod));

            // Format code (0 = text, 1 = binary)
            writer.Write(IPAddress.HostToNetworkOrder(col.FormatCode));
        }

        // Write length
        ms.Position = lengthPosition;
        var length = (int)(ms.Length - lengthPosition);
        writer.Write(IPAddress.HostToNetworkOrder(length));

        return ms.ToArray();
    }

    private static void WritePgString(BinaryWriter writer, string value)
    {
        foreach (char c in value)
        {
            writer.Write((byte)c);
        }
        writer.Write((byte)0);
    }

    /// <summary>
    /// Reads a null-terminated string from a byte array starting at the given offset.
    /// Updates the offset to point after the null terminator.
    /// </summary>
    public static string ReadPgString(byte[] buffer, ref int offset)
    {
        int start = offset;
        while (offset < buffer.Length && buffer[offset] != 0)
        {
            offset++;
        }
        string result = Encoding.ASCII.GetString(buffer, start, offset - start);
        offset++; // Skip null terminator
        return result;
    }

    /// <summary>
    /// Reads a big-endian int32 from a byte array starting at the given offset.
    /// Updates the offset to point after the value.
    /// </summary>
    public static int ReadInt32_BE(byte[] buffer, ref int offset)
    {
        int value = (buffer[offset] << 24) | (buffer[offset + 1] << 16) |
                    (buffer[offset + 2] << 8) | buffer[offset + 3];
        offset += 4;
        return value;
    }

    /// <summary>
    /// Reads a big-endian int16 from a byte array starting at the given offset.
    /// Updates the offset to point after the value.
    /// </summary>
    public static short ReadInt16_BE(byte[] buffer, ref int offset)
    {
        short value = (short)((buffer[offset] << 8) | buffer[offset + 1]);
        offset += 2;
        return value;
    }
}
