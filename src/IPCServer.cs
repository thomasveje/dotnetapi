// using System;
// using System.Collections.Generic;
// using System.IO.Pipes;
// using ProtoBuf;

// // https://protogen.marcgravell.com/
// public static class IPCServer
// {
//     public static event Action<Openiap.envelope>? onAwesomeMessage;
//     public static byte[] Take_Byte_Arr_From_Int(long Source_Num)
//     {
//         byte[] bytes = new byte[4];
//         bytes[3] = (byte)(Source_Num >> 24);
//         bytes[2] = (byte)(Source_Num >> 16);
//         bytes[1] = (byte)(Source_Num >> 8);
//         bytes[0] = (byte)Source_Num;
//         return bytes;
//     }
//     public static void Send(Openiap.envelope message)
//     {
//         if (server == null || !server.IsConnected) return;
//         using (var memoryStream = new MemoryStream())
//         {
//             message.seq = seq;
//             Serializer.Serialize(memoryStream, message);
//             var byteArray = memoryStream.ToArray();
//             server.Write(Take_Byte_Arr_From_Int(byteArray.Length), 0, 4);
//             Console.WriteLine("[SEND][" + message.command + "] size: " + byteArray.Length + " " + BitConverter.ToString(Take_Byte_Arr_From_Int(byteArray.Length)));
//             server.Write(byteArray, 0, byteArray.Length);
//             seq++;
//         }
//     }
//     public static byte[] pack<T>(T message)
//     {
//         using (var memoryStream = new MemoryStream())
//         {
//             Serializer.Serialize(memoryStream, message);
//             var byteArray = memoryStream.ToArray();
//             return byteArray;
//         }
//     }
//     public static T unpack<T>(byte[] byteArray)
//     {
//         using (var memoryStream = new MemoryStream(byteArray))
//         {
//             var message = Serializer.Deserialize<T>(memoryStream);
//             return message;
//         }
//     }
//     public static void sendFileContent(string rid, string filename)
//     {
//         var bs = new Openiap.Envelope() { command = "beginstream" };
//         bs.rid = rid;
//         IPCServer.Send(bs);
//         using (var inFileSteam = new FileStream(filename, FileMode.Open))
//         {
//             int bytesRead = 0;
//             do
//             {
//                 byte[] buffer = new byte[5 * 1024 * 1024]; // 5MB in bytes is 5 * 2^20
//                 bytesRead = inFileSteam.Read(buffer, 0, buffer.Length);
//                 var s = new Openiap.Envelope()
//                 {
//                     command = "stream",
//                     data = IPCServer.pack<Openiap.stream>(new Openiap.stream() { data = buffer })
//                 };
//                 s.rid = rid;
//                 IPCServer.Send(s);
//             } while (bytesRead > 0);
//         }

//         var es = new Openiap.Envelope() { command = "endstream" };
//         es.rid = rid;
//         IPCServer.Send(es);
//     }
//     private static Openiap.Envelope? ReadMessage(PipeStream stream)
//     {
//         if (stream == null || !stream.IsConnected) return null;
//         using (var ms = new MemoryStream())
//         {
//             var sizebuf = new byte[4];
//             stream.Read(sizebuf, 0, sizebuf.Length);
//             var size = (int)BitConverter.ToUInt32(sizebuf, 0);
//             if (size > 0)
//             {
//                 int bytesread = 0;
//                 var buffer = new byte[size];
//                 while (bytesread < size)
//                 {
//                     bytesread += stream.Read(buffer, bytesread, (size - bytesread));
//                 }
//                 var message = Serializer.Deserialize<Openiap.Envelope>(new ReadOnlySpan<byte>(buffer));
//                 Console.WriteLine("[RESC][" + message.command + "] size: " + size + " " + BitConverter.ToString(sizebuf));
//                 return message;
//             }
//             else if (size == 0)
//             {
//                 Console.WriteLine("size is 0, throw EndOfStreamException");
//                 return null;
//             }
//             else
//             {
//                 return null;
//             }
//         }
//     }
//     private static NamedPipeServerStream? server;
//     private static Queue<Openiap.Envelope> messageQueue = new Queue<Openiap.Envelope>();
//     private static bool processing = false;
//     private static int seq = 0;
//     public static void Start()
//     {
//         // compile the model - to avoid using reflection, for better speed
//         Serializer.PrepareSerializer<Openiap.Envelope>();
//         Task.Run(() =>
//         {
//             while (true)
//             {
//                 try
//                 {
//                     using (server = new NamedPipeServerStream("testpipe", PipeDirection.InOut, NamedPipeServerStream.MaxAllowedServerInstances,
//                         PipeTransmissionMode.Byte))
//                     {
//                         server.WaitForConnection();
//                         Console.WriteLine("Serving new client");
//                         Openiap.Envelope? msg;
//                         seq = 0;
//                         do
//                         {
//                             msg = ReadMessage(server);
//                             if (msg == null) continue;
//                             messageQueue.Enqueue(msg);
//                             if (!processing)
//                             {
//                                 try
//                                 {
//                                     processing = true;
//                                     do
//                                     {
//                                         var m = messageQueue.Dequeue();
//                                         if (onAwesomeMessage != null) onAwesomeMessage(m);
//                                     } while (messageQueue.Count > 0);
//                                 }
//                                 catch (System.Exception ex)
//                                 {
//                                     Console.WriteLine("Process queue error:" + ex.Message);
//                                 }
//                                 finally
//                                 {
//                                     processing = false;
//                                 }
//                             }
//                         } while (msg != null);
//                     }
//                 }
//                 catch (Exception ex)
//                 {
//                     Console.WriteLine(ex.ToString());
//                 }
//             }
//         });
//     }
// }
