using System.IO.Pipes;
using System.Diagnostics;
using System.Net.WebSockets;
using Openiap;

/*
sudo apt remove dotnet* -y
sudo apt remove aspnetcore* -y
sudo apt remove netstandard* -y
sudo apt remove dotnet-sdk-6.0 dotnet-sdk-7.0 -y
sudo apt autoremove -y
sudo apt clean
sudo apt update
sudo apt install dotnet-sdk-6.0 -y

wget https://packages.microsoft.com/config/ubuntu/22.10/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update &&   sudo apt-get install -y dotnet-sdk-7.0
sudo apt-get update &&   sudo apt-get install -y dotnet-sdk-6.0
sudo apt install dotnet6


*/


Console.Title = "Client - Connecting";
Console.WriteLine("create client");

 if(Environment.GetEnvironmentVariable("cli") == "true") {
    Console.WriteLine("CLI mode");
    await ConsoleHandler();
 } else {
    await AgentHandler();
 }
async Task ConsoleHandler() {
    var apiurl = Environment.GetEnvironmentVariable("apiurl");
    if(apiurl == null || apiurl == "") apiurl = Environment.GetEnvironmentVariable("grpcapiurl");
    if(apiurl == null || apiurl == "") throw new ArgumentException("apiurl not set");
    openiap client = new openiap(apiurl);
    client.OnDisconnected = async () => {
        Console.WriteLine("Disconnected");
        await Task.Delay(1000);
        await protowrap.Connect(client);
    };
    client.OnSignedin = async (user) =>
    {
        Console.Title = "Client - Signed in as " + user.Username;
        Console.WriteLine("Signed in as " + user.Username);
        try
        {
            // var result2 = await client.Watch("entities", new string[] { "$.]" }, (string operation, dynamic document) =>
            var result2 = await client.Watch("entities", new string[] {"$.[?(@._type == 'test')]"}, (string operation, dynamic document) => {
                Console.WriteLine("SERVER " + operation + " -> " + document["_id"] + " " + document["name"]);
            });
            Console.WriteLine("Created watch on entities with id " + result2);
        }
        catch (System.Exception ex)
        {
            Console.WriteLine("Failed to create watch " + ex.Message);
        }
        // return Task.CompletedTask;
    };
    await protowrap.Connect(client);
    do
    {
        try
        {
            string ?line = Console.ReadLine();
            if(line == null) line = "";
            if(line == "p") {
                var files = new string[]{ "/home/allan/Pictures/allan.png" };
                var result = await client.PushWorkitem("q2", new { test = "test" }, files);
                Console.WriteLine("Pushed workitem with id " + result.Id);

                var wi = await client.PopWorkitem("q2");
                Console.WriteLine("Poped workitem with id " + wi.Id);
                files = new string[]{ "/home/allan/Pictures/allan2.png" };
                wi.State = "successful";
                await client.UpdateWorkitem(wi, new { test2 = "test2" }, files);
                Console.WriteLine("Updated workitem with id " + wi.Id);
            } else if(line == "s") {
                var user = await client.Signin("testuser", "testuser");
                Console.Title = "Client - Signed in as " + user.Username;
                Console.WriteLine("Signed in as " + user.Username);
            } else if(line == "q") {
                // Environment.Exit(0);
                var results = await client.Query<dynamic>("entities", projection: new {_type=1, name=1}, top: 2);
                foreach(dynamic res in results) {
                    Console.WriteLine(res["_type"] + " " + res["name"]);
                }
            } else if (line == "qq") {
                var results = await client.Query<dynamic>("entities", projection: new {_type=1, name=1});
                foreach(dynamic res in results) {
                    Console.WriteLine(res["_type"] + " " + res["name"]);
                }
            } else if (line == "c") {
                var results = await client.Count("entities", "{}");
                Console.WriteLine("count: " + results);
            } else if (line == "ll") {
                var results = await client.ListCollections();
                foreach(dynamic res in results) {
                    Console.WriteLine(res["name"]);
                }
            } else if (line == "aa") {
                var pipe = new Dictionary<string, object>
                        {
                            ["$group"] = new { _id = "$_type", 
                            count = new Dictionary<string, object> { ["$sum"] = 1 }
                            }
                        };
                var results = await client.Aggregate("entities", pipe);
                foreach(dynamic res in results) {
                    Console.WriteLine(res["_id"] + " " + res["count"]);
                }
            } else if (line == "i") {
                var result = await client.InsertOne<dynamic>("entities", new { _type = "test", name = "test" });
                Console.WriteLine("inserted one with _id " + result["_id"]);
            } else if (line == "ii") {
                var items = new List<dynamic>();
                for(var i = 1; i <= 10; i++) {
                    items.Add(new { _type = "test", name = "test" + i });
                }
                var results = await client.InsertMany<dynamic>("entities", items.ToArray());
                for(var i = 1; i <= 10; i++) {
                    var result = results[i-1];
                    Console.WriteLine("inserted one with _id " + result["_id"]);
                }
            } else if (line == "iu") {
                var result = await client.InsertOrUpdateOne<dynamic>("entities", new { _type = "test", unique = "test", name = "update one" }, "unique");
                Console.WriteLine("inserted/updated one with _id " + result["_id"] + " and name " + result["name"]);
                var result2 = await client.InsertOrUpdateOne<dynamic>("entities", new { _type = "test", unique = "test", name = "update two" }, "unique");
                Console.WriteLine("inserted/updated one with _id " + result2["_id"] + " and name " + result2["name"]);
            } else if (line == "u") {
                var result = await client.InsertOne<dynamic>("entities", new { _type = "test", name = "test" });
                Console.WriteLine("inserted one with _id " + result["_id"] + " and name " + result["name"]);
                result["name"] = "test updated";
                var result2 = await client.UpdateOne<dynamic>("entities", result);
                Console.WriteLine("updated _id " + result2["_id"] + " to name " + result2["name"]);
            } else if (line == "uu") {
                var items = new List<dynamic>();
                for(var i = 1; i <= 10; i++) {
                    items.Add(new { _type = "test", name = "test" + i });
                }
                var results = await client.InsertMany<dynamic>("entities", items.ToArray());
                for(var i = 1; i <= 10; i++) {
                    var result = results[i-1];
                    Console.WriteLine("inserted one with _id " + result["_id"]);
                    result["name"] = "test updated " + i;
                }
                var result2 = await client.UpdateMany<dynamic>("entities", results.ToArray());
                for(var i = 1; i <= 10; i++) {
                    var result = result2[i-1];
                    Console.WriteLine("updated _id " + result["_id"] + " to name " + result["name"]);
                }
            } else if (line == "ud") {
                var doc = new Dictionary<string, object>
                        {
                            ["$set"] = new { name = "test updated" }
                        };
                var result = await client.UpdateDocument("entities", new { _type = "test" }, doc);
                Console.WriteLine(result.MatchedCount + " documents updated");
            } else if (line == "d") {
                var result = await client.InsertOne<dynamic>("entities", new { _type = "test", name = "test" });
                Console.WriteLine("inserted one with _id " + result["_id"] + " and name " + result["name"]);
                var result2 = await client.DeleteOne("entities", result["_id"].ToString());
                Console.WriteLine("deleted " + result2 + " documents");
            } else if (line == "dd") {
                var result = await client.DeleteMany("entities", new { _type = "test" });
                Console.WriteLine("deleted " + result + " documents");
            } else if (line == "w") {
                var result = await client.Watch("entities", new string[] {"$.[?(@._type == 'test')]"}, (string operation, dynamic document) => {
                    Console.WriteLine("SERVER " + operation + " -> " + document["_id"] + " " + document["name"]);
                });
                Console.WriteLine("Watch created as id " + result);
            } else if (line == "nn") {
                var result = await client.RegisterQueue("", async (Openiap.QueueEvent qe, dynamic payload) => {
                    Console.WriteLine(payload.name);
                    payload.name = "updated";

                    var results = await client.Query<dynamic>("entities", projection: new { _type = 1, name = 1 }, top: 2);
                    foreach (dynamic res in results)
                    {
                        Console.WriteLine(res["_type"] + " " + res["name"]);
                    }

                    return payload;
                });
                Console.WriteLine("RegisterQueue as " + result);
                var payload = await client.QueueMessage(result, "", new { test = "test", name = "find me" }, rpc: true);
                Console.WriteLine("RegisterQueue as " + payload);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    } while (true);

}
async Task<openiap> QuickConnect() {
    var apiurl = Environment.GetEnvironmentVariable("apiurl");
    if(apiurl == null || apiurl == "") apiurl = Environment.GetEnvironmentVariable("grpcapiurl");
    if(apiurl == null || apiurl == "") throw new ArgumentException("apiurl not set");
    TaskCompletionSource<openiap> tcs = new TaskCompletionSource<openiap>();

    openiap client = new openiap(apiurl);
    client.OnConnected = () =>
    {
        Console.WriteLine("connected!");
        return Task.CompletedTask;
    };
    client.OnSignedin = (user) =>
    {
        tcs.SetResult(client);
        return Task.CompletedTask;
    };
    await protowrap.Connect(client);
    await tcs.Task;
    return client;
}

async Task AgentHandler() {
    var gitrepo = Environment.GetEnvironmentVariable("gitrepo");
    var packageid = Environment.GetEnvironmentVariable("packageid");
    var WorkingDirectory = "package";
    if(!string.IsNullOrEmpty(gitrepo)) {
        if(!Directory.Exists("package")) {
            var gitclone = new Process();
            gitclone.StartInfo.FileName = "git";
            gitclone.StartInfo.Arguments = "clone " + gitrepo + " package";
            gitclone.Start();
            gitclone.WaitForExit();
        }
    } else if(!string.IsNullOrEmpty(packageid)) {
        var client = await QuickConnect();
        var packages = await client.Query<dynamic>("agents", query:new {_type="package", _id=packageid});
        dynamic p = null; string fileid = "";
        foreach(dynamic res in packages) {
            p = res;
            fileid = res["fileid"];
            Console.WriteLine(res["_type"] + " " + res["name"]);
        }
        if(p == null) throw new Exception("package " + packageid + " not found");
        if(string.IsNullOrEmpty(fileid)) throw new Exception("package " + packageid + " has no fileid");

        // if(packages.Length == 0) throw new Exception("package " + packageid + " not found");
        // if(string.IsNullOrEmpty(packages[0].fileid)) throw new Exception("package " + packageid + " has no fileid");
        var filename = await client.DownloadFile(fileid);
        if(filename.EndsWith(".tgz")) {
            TarExample.Tar.ExtractTarGz(filename, "package");
            System.IO.File.Delete(filename);
        }
        if(System.IO.Directory.Exists(System.IO.Path.Join("package", "package"))) {
            WorkingDirectory = System.IO.Path.Join("package", "package");
        }

    } else {
        throw new Exception("packageid and gitrepo not set, one of them must be set");
    }
    // start agent by running dotnet run
    var agent = new Process();
    agent.StartInfo.FileName = "dotnet";
    agent.StartInfo.Arguments = "run";
    agent.StartInfo.WorkingDirectory = WorkingDirectory;
    agent.Start();
    agent.WaitForExit();
}

