using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Openiap;

public static class protowrap
{
    public static Dictionary<string, TaskCompletionSource<Envelope>> promises = new Dictionary<string, TaskCompletionSource<Envelope>>();
    // private async static Task _SendMessage(openiap client)
    // {
    //     if(client == null || client.grpcstream == null) return;
    //     while (client.sendqueue.Count > 0)
    //     {
    //         Envelope envelope;
    //         if(client.sendqueue.TryPeek(out envelope)) {
    //             try
    //             {
    //                 // Console.WriteLine("Sending message " + envelope.Command);
    //                 await client.grpcstream.RequestStream.WriteAsync(envelope);
    //                 client.sendqueue.TryDequeue(out envelope);
    //             }
    //             catch (System.Exception ex)
    //             {
    //                 // Console.WriteLine("Error sending message " + envelope.Command + " " + ex.Message);
    //                 if(envelope != null && (envelope.Command == "ping" || envelope.Command == "signin")) {
    //                     if(client.sendqueue.TryDequeue(out envelope)) {
    //                         if(promises.ContainsKey(envelope.Id)) {
    //                             promises[envelope.Id].SetException(new System.Exception("Connection closed"));
    //                             promises.Remove(envelope.Id);
    //                         }
    //                     }                        
    //                 }
    //                 // try
    //                 // {
    //                 // await client.grpcstream.RequestStream.CompleteAsync();
    //                 // }
    //                 // catch (System.Exception)
    //                 // {
    //                 // }
    //                 if(client != null && client.cts != null) client.cts.Cancel();
    //                 return;
    //             }
    //         }
    //     }
    // }
    private async static Task SendMessage(openiap client, Envelope envelope)
    {
        if (client.grpcstream != null)
        {
            // client.sendqueue.Enqueue(envelope);
            // await _SendMessage(client);
            try
            {
                await client.grpcstream.RequestStream.WriteAsync(envelope);
            }
            catch (Grpc.Core.RpcException ex) {
                // Console.WriteLine(ex.Message);
            }
            catch (System.Exception)
            {
                throw;
            }
        } else {
            throw new Exception("Unknown client type");
        }
    }
    public async static Task Ping(openiap client)
    {
        var any = Any.Pack(new PingRequest() { },
        PingRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "ping", Data = any };
        await protowrap.SendMessage(client, envelope);
    }
    private async static Task grpc_connact_and_listener(openiap client)
    {
        try
        {
            if(client.grpc == null) return;
            Uri u = new Uri(client.apiurl);
            Console.WriteLine("Setting up server stream " + u.Host + ":" + u.Port);
            client.grpcstream = client.grpc.SetupStream();
            client.cts = new CancellationTokenSource();
            string username = u.UserInfo.Contains(":") ? u.UserInfo.Split(':')[0] : "";
            string password = u.UserInfo.Contains(":") ? u.UserInfo.Split(':')[1] : "";
            string jwt = Environment.GetEnvironmentVariable("jwt");
            if (username != "" && password != "" && client.autologin)
            {
                _ = Task.Run(async () =>
                {
                    await client.Signin(username, password);
                });
            } else if ( jwt != null && jwt != "" && client.autologin) {
                _ = Task.Run(async () =>
                {
                    await client.Signin(jwt);
                });
            }
            if(client.OnConnected != null) {
                _ = Task.Run(async () => {
                    await client.OnConnected();
                });
            }
            while (await client.grpcstream.ResponseStream.MoveNext(client.cts.Token))
            {
                var response = client.grpcstream.ResponseStream.Current;
                if (response == null)
                {
                    Console.WriteLine("Got null response");
                } else if (response.Command == null || response.Command == "") {
                    Console.WriteLine("Got response with no command");
                } else if (promises.ContainsKey(response.Rid)) {
                    if (response.Data.TypeUrl.Contains("ErrorResponse"))
                    {
                        var error = response.Data.Unpack<ErrorResponse>();
                        promises[response.Rid].SetException(new System.Exception("SERVER ERROR " + error.Message));
                        promises.Remove(response.Rid);
                    } else if (response.Data.TypeUrl.Contains("PingResponse")) {
                        promises[response.Rid].SetResult(response);
                        promises.Remove(response.Rid);
                    } else {
                        promises[response.Rid].SetResult(response);
                        promises.Remove(response.Rid);
                    }
                } else if (response.Command == "refreshtoken") {
                    var rt = response.Data.Unpack<RefreshToken>();
                    if(rt != null) {
                        client.user = rt.User;
                        client.jwt = rt.Jwt;
                    }
                } else if (response.Command == "watchevent") {
                    var watchevent = response.Data.Unpack<WatchEvent>();
                    if (client.watchers.ContainsKey(watchevent.Id)) {
                        client.watchers[watchevent.Id].Invoke(watchevent.Operation,
                            Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(watchevent.Document));
                    }
                } else if (response.Command == "queueevent") {
                    var queueevent = response.Data.Unpack<QueueEvent>();

                    var payload = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(queueevent.Data);
                    if(payload != null && payload.ContainsKey("payload")) {
                        payload = payload["payload"] as Dictionary<string, object>;
                    } else if (payload != null) {
                        if(payload.ContainsKey("__jwt")) payload.Remove("__jwt");
                        if(payload.ContainsKey("__user")) payload.Remove("__user");
                        if(payload.ContainsKey("traceId")) payload.Remove("traceId");
                        if(payload.ContainsKey("spanId")) payload.Remove("spanId");
                    }
                    var json = Newtonsoft.Json.JsonConvert.SerializeObject(payload);
                    var pay = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
                    if (client.QueueReplies.ContainsKey(queueevent.CorrelationId) && queueevent.Replyto == "") {
                        client.QueueReplies[queueevent.CorrelationId].SetResult(pay);
                        client.QueueReplies.Remove(queueevent.CorrelationId);
                    } else if (client.amqpQueues.ContainsKey(queueevent.Queuename)) {
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                var resultpayload = await client.amqpQueues[queueevent.Queuename].Invoke(queueevent, pay);
                                if (resultpayload != null && queueevent.Replyto != "")
                                {
                                    _ = client.QueueMessage(queueevent.Replyto, "", resultpayload, correlationId: queueevent.CorrelationId);
                                }
                            }
                            catch (System.Exception ex)
                            {
                                Console.WriteLine(ex.ToString());
                            }
                        });
                    } 
                } else if (response.Command == "ping") {
                    // Console.WriteLine("Server pinged me");
                } else if (response.Command == "error") {
                    var error = response.Data.Unpack<ErrorResponse>();
                    Console.WriteLine("Server Error " + error.Message + "\n" + error.Stack);
                } else {
                    Console.WriteLine("Unknown response " + response.Command);
                }

            }
        }
        catch (Grpc.Core.RpcException ex) {
            // Console.WriteLine(ex.Message);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
        foreach (var promise in promises.ToList())
        {
            promise.Value.SetException(new System.Exception("Connection closed"));
            promises.Remove(promise.Key);
        }
        foreach (var key in client.TempQueueNames)
        {
            if(client.QueueReplies.ContainsKey(key)) {
                client.QueueReplies[key].SetException(new System.Exception("Connection closed"));
                client.QueueReplies.Remove(key);
            }
            client.amqpQueues.Remove(key);
        }
        client.TempQueueNames.Clear();
        client.watchers.Clear();
        if (client.OnDisconnected != null)
        {
            _ = Task.Run(async () =>
            {
                await client.OnDisconnected();
            });
        }
    }
    public async static Task<openiap> Connect(openiap client)
    {
        await Task.Delay(1); // to shut up the linter about using async and not using await
        Uri u = new Uri(client.apiurl);
        if (u.Scheme == "grpc")
        {
            GrpcChannel channel;
            GrpcChannelOptions options = new GrpcChannelOptions();
            options.MaxReceiveMessageSize = 256 * 1024 * 1204;
            // ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            // options.LoggerFactory = loggerFactory;
            if (u.Port == 443)
            {
                var url = "https://" + u.Authority;
                options.Credentials = new Grpc.Core.SslCredentials();
                channel = GrpcChannel.ForAddress(url, options);
            }
            else
            {
                var url = "http://" + u.Authority;
                options.Credentials = Grpc.Core.ChannelCredentials.Insecure;
                channel = GrpcChannel.ForAddress(url, options);
            }

            client.grpc = new Openiap.FlowService.FlowServiceClient(channel);
            _ = Task.Run(async () => { await grpc_connact_and_listener(client); });
        }
        return client;
    }
    public static async Task<Envelope> RPC(openiap client, Envelope envelope)
    {
        var id = Guid.NewGuid().ToString().Substring(0, 8);
        envelope.Id = id;
        promises.Add(id, new TaskCompletionSource<Envelope>(TaskCreationOptions.RunContinuationsAsynchronously));
        await SendMessage(client, envelope);
        var result = await promises[id].Task;
        promises.Remove(id);
        return result;
    }
}
