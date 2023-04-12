using System.Collections.Concurrent;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Openiap;

public class openiap {
    internal string apiurl = "";
    public bool autologin { get; set;} = true;
    public openiap(string apiurl) {
        this.apiurl = apiurl;
        _ = Task.Run(async () => { 
            var errorcounter = 0;
            while(true) {
                await Task.Delay(10000);
                try
                {
                    await this.Ping();
                }
                catch (System.Exception ex)
                {
                    errorcounter++;
                    Console.WriteLine("PING ERROR: "+ ex.Message);
                }
                if(errorcounter > 5) {
                    this.cts?.Cancel();
                }
            }
        });
    }
    public Func<Task> ?OnConnected = null;
    public Func<User, Task> ?OnSignedin = null;
    public Func<Task> ?OnDisconnected = null;
    public FlowService.FlowServiceClient? grpc { get; set; } = null;
    public AsyncDuplexStreamingCall<Envelope, Envelope> ?grpcstream { get; set; } = null;
    public User ?user { get; internal set; } = null;
    public string jwt { get; internal set; } = "";
    public CancellationTokenSource ?cts { get; internal set; } = null;
    public Dictionary<string, System.IO.Stream> Streams = new Dictionary<string, System.IO.Stream>();
    internal ConcurrentQueue<Envelope> sendqueue = new ConcurrentQueue<Envelope>();

    public async Task Ping()
    {
        await protowrap.Ping(this);
    }
   public async Task<User> Signin(string username, string password, bool ping = true, bool validateonly = false, bool longtoken = false) {
        var any = Any.Pack(new SigninRequest() { Username = username, Password = password, Ping = ping, Validateonly = validateonly, Longtoken = longtoken },
            SigninRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "signin", Data = any };
        var result = await protowrap.RPC(this, envelope);
        var reply = result.Data.Unpack<SigninResponse>();
        if(!validateonly) {
            this.user = reply.User;
            this.jwt = reply.Jwt;
            if(this.OnSignedin != null) {
                _ = Task.Run(async () => {
                    await this.OnSignedin(reply.User);
                });
            }
        }
        return reply.User;
    }
   public async Task<User> Signin(string jwt, bool ping = true, bool validateonly = false, bool longtoken = false) {
        var any = Any.Pack(new SigninRequest() { Jwt = jwt, Ping = ping, Validateonly = validateonly, Longtoken = longtoken },
            SigninRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "signin", Data = any };
        var result = await protowrap.RPC(this, envelope);
        var reply = result.Data.Unpack<SigninResponse>();
        if(!validateonly) {
            this.user = reply.User;
            this.jwt = reply.Jwt;
            if(this.OnSignedin != null) {
                _ = Task.Run(async () => {
                    await this.OnSignedin(reply.User);
                });
            }
        }
        return reply.User;
    }
    public async Task<dynamic[]> ListCollections(bool includehist = false) {
        var any = Any.Pack(new ListCollectionsRequest() { Includehist = includehist },
            ListCollectionsRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "listcollections", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<ListCollectionsResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic[]>(reply.Results);
        if(result == null) return new dynamic[] {};
        return result;
    }
    public async Task DropCollection(string collectionname) {
        var any = Any.Pack(new DropCollectionRequest() { Collectionname = collectionname },
            DropCollectionRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "dropcollection", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
    }
    public async Task<T[]> Query<T>(string collectionname, object ?query = null, object ?projection = null, 
    int top = 100, int skip = 0, object ?orderby = null, string queryas = "") {
        string s = "";
        // if sort is of type string
        if(orderby != null && orderby.GetType() == typeof(string)) {
            s = (string)orderby;
        } else if (orderby != null) {
            s = Newtonsoft.Json.JsonConvert.SerializeObject(orderby);
        }
        var q = query == null ? "{}" : Newtonsoft.Json.Linq.JObject.FromObject(query).ToString();
        var qr = new QueryRequest() { Collectionname = collectionname, Query = q, Top = top, Skip = skip, 
        Orderby = s, Queryas = queryas };
        if(projection != null) {
            qr.Projection = Newtonsoft.Json.Linq.JObject.FromObject(projection).ToString();
        }
        var any = Any.Pack(qr ,
            QueryRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "query", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<QueryResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<T[]>(reply.Results);
        if(result == null) return Array.Empty<T>();
        return result;
    }
    public async Task<int> Count(string collectionname, string query, string queryas = "") {
        var any = Any.Pack(new CountRequest() { Collectionname = collectionname, Query = query, Queryas = queryas },
            CountRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "count", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<CountResponse>();
        return reply.Result;
    }
    public async Task<dynamic[]> Aggregate(string collectionname, object aggregates, string queryas = "")  {
        var any = Any.Pack(new AggregateRequest() { Collectionname = collectionname, 
            Aggregates = Newtonsoft.Json.JsonConvert.SerializeObject(aggregates), Queryas = queryas },
            AggregateRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "aggregate", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<AggregateResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic[]>(reply.Results);
        if(result == null) return new dynamic[] {};
        return result;
    }
    public async Task<T> InsertOne<T>(string collectionname, T item)  {
        var any = Any.Pack(new InsertOneRequest() { Collectionname = collectionname, 
            Item = Newtonsoft.Json.JsonConvert.SerializeObject(item) },
            InsertOneRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "insertone", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<InsertOneResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(reply.Result);
        if(result == null) throw new Exception("InsertOne failed (null result)");
        return result;
    }
    public async Task<T[]> InsertMany<T>(string collectionname, T[] items, bool skipresults = false ){
        var any = Any.Pack(new InsertManyRequest() { Collectionname = collectionname, 
            Items = Newtonsoft.Json.JsonConvert.SerializeObject(items), Skipresults = skipresults },
            InsertManyRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "insertmany", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<InsertManyResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<T[]>(reply.Results);
        if(skipresults || result == null) return new T[] {};
        return result;
    }
    public async Task<T> UpdateOne<T>(string collectionname, T item) {
        var any = Any.Pack(new UpdateOneRequest() { Collectionname = collectionname,  
            Item = Newtonsoft.Json.JsonConvert.SerializeObject(item)},
            UpdateOneRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "updateone", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<UpdateOneResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(reply.Result);
        if(result == null) throw new Exception("UpdateOne failed (null result)");
        return result;
    }
    public async Task<T[]> UpdateMany<T>(string collectionname, T[] items, bool skipresults = false) {
        return await InsertOrUpdateMany<T>(collectionname, items, "_id", skipresults);
    }
    public async Task<T> InsertOrUpdateOne<T>(string collectionname, T item, string uniqeness = "_id") {
        var any = Any.Pack(new InsertOrUpdateOneRequest() { Collectionname = collectionname, 
            Item = Newtonsoft.Json.JsonConvert.SerializeObject(item), Uniqeness = uniqeness },
            InsertOrUpdateOneRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "insertorupdateone", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<InsertOrUpdateOneResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(reply.Result);
        if(result == null) throw new Exception("InsertOrUpdateOne failed (null result)");
        return result;
    }
    public async Task<T[]> InsertOrUpdateMany<T>(string collectionname, T[] items, string uniqeness = "_id", bool skipresults = false) {
        var any = Any.Pack(new InsertOrUpdateManyRequest() { Collectionname = collectionname, 
            Items = Newtonsoft.Json.JsonConvert.SerializeObject(items), Skipresults = skipresults, Uniqeness = uniqeness },
            InsertOrUpdateManyRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "insertorupdatemany", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<InsertOrUpdateManyResponse>();
        var result = Newtonsoft.Json.JsonConvert.DeserializeObject<T[]>(reply.Results);
        if(skipresults || result == null) return new T[] {};
        return result;
    }
    public async Task<UpdateResult> UpdateDocument(string collectionname, object query, object documment)  {
        var any = Any.Pack(new UpdateDocumentRequest() { Collectionname = collectionname, 
            Query = Newtonsoft.Json.JsonConvert.SerializeObject(query), Document = Newtonsoft.Json.JsonConvert.SerializeObject(documment) },
            UpdateDocumentRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "updatedocument", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<UpdateDocumentResponse>();
        return reply.Opresult;
    }
    public async Task<int> DeleteOne(string collectionname, string id) {
        var any = Any.Pack(new DeleteOneRequest() { Collectionname = collectionname, 
            Id = id },
            DeleteOneRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "deleteone", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<DeleteOneResponse>();
        return reply.Affectedrows;
    }
    public async Task<int> DeleteMany(string collectionname, object query, bool recursive = false) {
        var any = Any.Pack(new DeleteManyRequest() { Collectionname = collectionname, 
            Query = Newtonsoft.Json.JsonConvert.SerializeObject(query), Recursive = recursive },
            DeleteManyRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "deletemany", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<DeleteManyResponse>();
        return reply.Affectedrows;
    }
    internal Dictionary<string, Action<string, dynamic>> watchers = new Dictionary<string, Action<string, dynamic>>();
    public async Task<string> Watch(string collectionname,string[] paths, Action<string, dynamic> callback) {
        var w = new WatchRequest() { Collectionname = collectionname };
        foreach (var p in paths) w.Paths.Add(p);
        var any = Any.Pack(w, WatchRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "watch", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<WatchResponse>();
        if(reply.Id != "") {
            watchers[reply.Id] = callback;
            Console.WriteLine("watcher added: " + reply.Id + " i now have " + watchers.Count + " watchers");
        }
        return reply.Id;
    }
    public async Task Unwatch(string id) {
        var any = Any.Pack(new UnWatchRequest() { Id = id },
            UnWatchRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "unwatch", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<UnWatchResponse>();
        if(watchers.ContainsKey(id)) watchers.Remove(id);
    }
    public Dictionary<string, TaskCompletionSource<dynamic>> QueueReplies = new Dictionary<string, TaskCompletionSource<dynamic>>();
    public async Task<dynamic?> QueueMessage(string queuename, string exchangename, dynamic payload, bool striptoken = true, string correlationId = "", bool rpc = false) {
        var replyto = "";
        if(rpc == true) {
            correlationId = Guid.NewGuid().ToString().Substring(0, 8);
            QueueReplies.Add(correlationId, new TaskCompletionSource<dynamic>(TaskCreationOptions.RunContinuationsAsynchronously));
            replyto = replyqueue;
        }
        var any = Any.Pack(new QueueMessageRequest() { Queuename = queuename, Exchangename = exchangename, Data = Newtonsoft.Json.JsonConvert.SerializeObject(payload) 
        , Striptoken = striptoken, CorrelationId= correlationId, Replyto = replyto},
            QueueMessageRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "queuemessage", Data = any };
        await protowrap.RPC(this, envelope);
        if(rpc == true) {
            var result = await QueueReplies[correlationId].Task;
            QueueReplies.Remove(correlationId);
            return result;
        }
        return null;
    }
    private string replyqueue = "";
    internal Dictionary<string, Func<QueueEvent, dynamic, Task<dynamic>>> amqpQueues = new Dictionary<string, Func<QueueEvent, dynamic, Task<dynamic>>>();
    internal List<string> TempQueueNames = new List<string>();
    public async Task<string> RegisterQueue(string queuename, Func<QueueEvent, dynamic, Task<dynamic>> callback) {
        var any = Any.Pack(new RegisterQueueRequest() { Queuename = queuename},
            RegisterQueueRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "registerqueue", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<RegisterQueueResponse>();
        if(reply.Queuename != "") {
            amqpQueues[reply.Queuename] = callback;
        }
        if(queuename == "") TempQueueNames.Add(reply.Queuename);
        if(replyqueue == "") replyqueue = reply.Queuename;
        return reply.Queuename;
    }
    public async Task<Workitem> PushWorkitem(string wiq, dynamic payload, string[] ?addfiles  = null, Workitem ?wi = null) {
        if(wi == null) wi = new Workitem();
        if(addfiles == null) addfiles = new string[0];
        foreach(var f in addfiles) {
            var exists = wi.Files.Where(x => x.Filename == f).FirstOrDefault();
            if(exists == null) {
                wi.Files.Add(new WorkitemFile() { Filename = f });
            } else {
                exists.File = Google.Protobuf.ByteString.CopyFrom(await File.ReadAllBytesAsync(f));
            }
        }
        foreach(var wif in wi.Files) {
            if(wif.File.Length == 0 && wif.Filename != "") {
                wif.File = Google.Protobuf.ByteString.CopyFrom(await File.ReadAllBytesAsync(wif.Filename));
            }
        }
        if(wi.Payload == null && wi.Payload != "") {
            var p = wi.Payload;
            if(p != null) {
                var tmp = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(p);
                if(tmp != null) payload = tmp;
            }
        }
        var pwir = new PushWorkitemRequest() { Wiq = wiq, FailedWiq = wi.FailedWiq, SuccessWiq = wi.SuccessWiq, 
            Name = wi.Name, Nextrun = wi.Nextrun, Priority = wi.Priority, Payload = Newtonsoft.Json.JsonConvert.SerializeObject(payload)
             };
        foreach(var f in wi.Files) {
            pwir.Files.Add(new WorkitemFile() { Filename = f.Filename, File = f.File });
        }
        var any = Any.Pack(pwir,
            PushWorkitemRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "pushworkitem", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<PushWorkitemResponse>();
        return reply.Workitem;
    }
    public async Task<Workitem> PopWorkitem(string wiq, bool includefiles = true ) {
        var pwir = new PopWorkitemRequest() { Wiq = wiq, Includefiles = includefiles, Compressed = false };
        var any = Any.Pack(pwir,
            PopWorkitemRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "popworkitem", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<PopWorkitemResponse>();
        if(reply.Workitem != null && reply.Workitem.Files != null) {
            foreach(var f in reply.Workitem.Files) {
                if(f.File.Length > 0) {
                    System.IO.File.WriteAllBytes(f.Filename, f.File.ToByteArray());
                }
            }
        }
        return reply.Workitem;
    }
    public async Task<Workitem> UpdateWorkitem(Workitem wi, dynamic ?payload = null, string[] ?addfiles = null) {
        if(addfiles == null) addfiles = new string[0];
        if(payload != null) {
            wi.Payload = Newtonsoft.Json.JsonConvert.SerializeObject(payload);
        }
        var pwir = new UpdateWorkitemRequest() { Workitem = wi };
        foreach(var f in addfiles) {
            pwir.Files.Add(new WorkitemFile() { 
                Filename = f,
                File = Google.Protobuf.ByteString.CopyFrom(await File.ReadAllBytesAsync(f))
                });
        }
        foreach(var f in wi.Files) {
            f.File = Google.Protobuf.ByteString.CopyFrom();
        }
        var any = Any.Pack(pwir, UpdateWorkitemRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "updateworkitem", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
        var reply = rpcresult.Data.Unpack<UpdateWorkitemResponse>();
        return reply.Workitem;
    }        
    public async Task DeleteWorkitem(string id ) {
        var pwir = new DeleteWorkitemRequest() { Id = id };
        var any = Any.Pack(pwir, DeleteWorkitemRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "deleteworkitem", Data = any };
        var rpcresult = await protowrap.RPC(this, envelope);
    }
    public async Task<string> DownloadFile(string id) {
        var pwir = new DownloadRequest() { Id = id };
        var any = Any.Pack(pwir, DownloadRequest.Descriptor.FullName);
        var envelope = new Envelope() { Command = "download", Data = any };
        var rid = Guid.NewGuid().ToString().Substring(0, 8);

        var filename = System.IO.Path.GetTempFileName();
        Envelope rpcresult;
        using (var inFileSteam = new FileStream(filename, FileMode.Create, FileAccess.Write, FileShare.None, 4096, true))
        {
            protowrap.SetStream(this, rid, inFileSteam);
            rpcresult = await protowrap.RPC(this, rid, envelope);
        }
        try
        {
            var reply = rpcresult.Data.Unpack<DownloadResponse>();
            System.IO.File.Copy(filename, reply.Filename, true);
            return reply.Filename;
        }
        catch (System.Exception)
        {
        }
            var reply2 = rpcresult.Data.Unpack<EndStream>();
            return "";
            // System.IO.File.Copy(filename, reply.Filename, true);
            // return reply.Filename;
    }
}