// Imports
using Newtonsoft.Json;
using System;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using System.Reflection.Metadata;
using System.Diagnostics.Metrics;
using System.Collections.Generic;

namespace NoahSQL
{
    // Send notifications
    class Sys
    {
        public void send(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[SYSTEM]: {msg}");
            Console.ResetColor();
        }
    }

    // Setup database
    class setupDatabase
    {
        public void initaliseDatabase(string DBname)
        {
            if (!Directory.Exists(DBname))
            {
                Directory.CreateDirectory(DBname);
                Sys Sys = new Sys();
                Sys.send("Created Database " + DBname);
            }
        }
    }

    class QueryInputEngine
    {
        // Important Variables for SQL intrepeter
        static Sys sys = new Sys();
        static Boolean isError = false;
        static dynamic res;

        public static async void searchEngine(string[] splitWords, string DB_NAME, string SELECT, string FROM, string WHERE)
        {
            // Variables
            List<String> listOfSelect = new List<String>();
            String dir = System.IO.Directory.GetCurrentDirectory();
            String table = @dir + @"\" + DB_NAME + @"\" + FROM + ".json";

            // Operation
            String oper1 = null;
            String oper2 = null;
            int oper_id = 0;

            // Checking if table exists
            if (File.Exists(table))
            {
                try
                {
                    // Check if it is single select statement
                    if (!SELECT.Contains(",")) listOfSelect.Add(SELECT);

                    // Check for multiple select statements
                    for (int x = 0; x < splitWords.Length; x++)
                    {
                        if (SELECT.Contains(","))
                        {
                            if (splitWords[x].Contains(",")) listOfSelect.Add(splitWords[x].Replace(",", ""));
                            if (splitWords[x] == "FROM") listOfSelect.Add(splitWords[x - 1]);
                        }

                        // Check for WHERE operation statement
                        if (WHERE is string)
                        {
                            if (x == splitWords.Length - 1) oper2 = splitWords[x];
                            if (x == splitWords.Length - 3) oper1 = splitWords[x];
                        }
                    }

                    // Read file
                    using (StreamReader reader = new StreamReader(table))
                    {
                        string line = await reader.ReadLineAsync();
                        Dictionary<int, string> idsToSearch = new Dictionary<int, string>();
                        column preDefinedValues = JsonConvert.DeserializeObject<column>(line);

                        int counter = 0;

                        // Getting ids to search in database
                        foreach (string select in listOfSelect)
                        {
                            counter = 0;
                            foreach (string preDefined in preDefinedValues.values.Keys)
                            {
                                if (select == preDefined) idsToSearch.Add(counter, select);
                                if (preDefined == oper1) oper_id = counter;
                                counter++;
                            }
                        }

                        // Skip the predefined values && prepare response message
                        line = await reader.ReadLineAsync();
                        res = new List<Dictionary<string, string>>();

                        // Reading row by row to check for values
                        while ((line = await reader.ReadLineAsync()) != null)
                        {
                            row values = JsonConvert.DeserializeObject<row>(line);
                            List<int> indexToSearch = new List<int>();

                            // If where condition
                            if (oper1 is string && oper2 is string)
                            {
                                if (values.values[oper_id] == oper2)
                                {
                                    Dictionary<string, string> data = new Dictionary<string, string>();
                                    
                                    foreach (int id in idsToSearch.Keys)
                                    {
                                        data.Add(idsToSearch[id], values.values[id]);
                                    }

                                    res.Add(data);
                                }
                            }
                            else
                            {  
                                Dictionary<string, string> data = new Dictionary<string, string>();
                                
                                foreach (int id in idsToSearch.Keys)
                                {
                                    data.Add(idsToSearch[id], values.values[id]);
                                }

                                res.Add(data);
                            }
                        }

                        // Converting to string
                        res = JsonConvert.SerializeObject(res);
                    }
                }
                catch (Exception e)
                {
                    sys.send($"Error retrieving {SELECT} from table {FROM}");
                    sys.send(e.ToString());
                    res = $"Error retrieving {SELECT} from table {FROM}";
                }
            }
            else
            {
                sys.send($"Table {FROM} doesnt exists");
                res = $"Table {FROM} doesnt exists";
            }
        }

        public static void createNewTable(string[] splitWords, string DB_NAME, string tableName)
        {
            // Variables
            column json = new column();
            JsonSerializer packagedValue = new JsonSerializer();

            Dictionary<string, string> values = new Dictionary<string, string>();
            Dictionary<string, string> valueAssignment = new Dictionary<string, string>();
            List<int> IDs = new List<int>();

            StringBuilder cleanValue = new StringBuilder();
            StringBuilder cleanValue2 = new StringBuilder();

            String dir = System.IO.Directory.GetCurrentDirectory();
            String table = @dir + @"\" + DB_NAME + @"\" + tableName + ".json";

            int counter = 0;

            // Checking if table exists
            if (!File.Exists(table))
            {
                try
                {
                    for (int x = 0; x < splitWords.Length; x++)
                    {
                        if (splitWords[x].Contains("int") || splitWords[x].Contains("str"))
                        {
                            // Filter / Clean out characters
                            cleanValue = new StringBuilder(splitWords[x]);
                            cleanValue2 = new StringBuilder(splitWords[x + 1]);

                            cleanValue.Replace(",", ""); cleanValue2.Replace(",", "");
                            cleanValue.Replace("(", ""); cleanValue2.Replace(")", "");
                            cleanValue.Replace(")", ""); cleanValue2.Replace(")", "");

                            values.Add(cleanValue2.ToString(), cleanValue.ToString());
                            IDs.Add(counter);

                            counter++;
                        }
                    }

                    // Prepare JSON in package and insert the values into the newly created table
                    json.values = values;
                    json.id = IDs;

                    packagedValue = new JsonSerializer();
                }
                catch (Exception e)
                {
                    isError = true;
                    Console.WriteLine(e.ToString());
                    sys.send($"There was an error creating table {tableName}.\n");
                    res = $"There was an error creating table {tableName}.\n";
                }

                // Error handler
                if (!isError)
                {
                    using (StreamWriter file = File.CreateText(table))
                    {
                        packagedValue.Serialize(file, json);
                        file.WriteLineAsync("\n");
                        file.Close();
                    }
                    sys.send("Created Table " + tableName + " and inserted values. \n");
                    res = "Created Table " + tableName + " and inserted values. \n";
                }
            }
            else
            {
                sys.send("Table already exists\n");
                res = "Table already exists\n";
            }
        }


        public static void insertToTable(string DB_NAME, Dictionary<string, string> keyWords, string[] splitWords)
        {
            // List of variables
            List<dynamic> valuesToBeAdded = new List<dynamic>();
            string dir = System.IO.Directory.GetCurrentDirectory();

            String table = @dir + @"\" + DB_NAME + @"\" + keyWords["INTO"] + ".json";
            StringBuilder cleanValue = new StringBuilder();

            if (File.Exists(table))
            {
                using (StreamWriter sw = File.AppendText(table))
                {
                    for (int x = 0; x < splitWords.Length; x++)
                    {
                        // Cleaning value, and converting it to right value
                        if (splitWords[x].Contains(",") || splitWords[x].Contains("(") || splitWords[x].Contains(")"))
                        {
                            cleanValue = new StringBuilder(splitWords[x]);

                            cleanValue.Replace(",", "");
                            cleanValue.Replace("(", "");
                            cleanValue.Replace(")", "");

                            convertToType(cleanValue.ToString(), DB_NAME, valuesToBeAdded, null);
                        }
                    }

                    // Packaging value and sending it to table
                    if (!isError)
                    {
                        row json = new row();
                        json.values = valuesToBeAdded;
                        var packagedValue = JsonConvert.SerializeObject(json);

                        sw.WriteLine(packagedValue);
                        sw.Close();

                        valuesToBeAdded.Clear();
                        sys.send("Added values to table " + keyWords["INTO"] + "\n");
                        res = "Added values to table " + keyWords["INTO"] + "\n";
                    }
                    else
                    {
                        valuesToBeAdded.Clear();
                        sw.Close();
                    }
                }
            }
            else
            {
                sys.send(keyWords["INTO"] + " is not a valid table.\n");
                res = keyWords["INTO"] + " is not a valid table.\n";
            }
        }
        public static void convertToType(string type, string table, List<dynamic> valuesToBeAdded, dynamic returnValue)
        {
            // Converts to type
            Sys Sys = new Sys();
            String firstReplace = "";
            String secondReplace = "";
            StringBuilder cleanedValue = new StringBuilder(type);

            if (type.Contains("'"))
            {
                firstReplace = "'";
                secondReplace = "'";
            }
            else if (type.Contains("\""))
            {
                firstReplace = "\"";
                firstReplace = "\"";
                secondReplace = "\"";
            }

            if (firstReplace == "" || secondReplace == "")
            {
                firstReplace = "";
                secondReplace = "";
            }

            try
            {
                // Format it to string or int, only supported values.
                if (firstReplace == "")
                {
                    int e = int.Parse(type);
                    valuesToBeAdded.Add(e);
                }
                else
                {
                    Console.WriteLine(firstReplace);
                    cleanedValue.Replace(firstReplace, "");
                    cleanedValue.Replace(secondReplace, "");
                    valuesToBeAdded.Add(cleanedValue.ToString());
                }
            }
            catch (Exception e)
            {
                Sys.send($"There was an error with uploading value {valuesToBeAdded} to table {table}. Current supported formats: INT (1,2,3,4) OR STR (\"\" OR '')");
                res = $"There was an error with uploading value {valuesToBeAdded} to table {table}. Current supported formats: INT (1,2,3,4) OR STR (\"\" OR '')";
                isError = true;
                valuesToBeAdded.Clear();
            }

        }
        public void query(string q, string DB_NAME, bool distributedFiles)
        {
            // Variables
            Dictionary<string, string> keyWords = new Dictionary<string, string>();

            q.Replace("--execute", "");

            string[] splitWords = q.Split(' ');
            string dir = System.IO.Directory.GetCurrentDirectory();

            keyWords.Add("SELECT", null);
            keyWords.Add("FROM", null);
            keyWords.Add("WHERE", null);
            keyWords.Add("INSERT", null);
            keyWords.Add("INTO", null);
            keyWords.Add("CREATE", null);
            keyWords.Add("TABLE", null);

            // Splititng string up and assinging key words to their values
            for (int x = 0; x < splitWords.Length; x++)
            {
                if (keyWords.ContainsKey(splitWords[x]))
                {
                    if (splitWords[x] == "SELECT")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["SELECT"] = NODE_LEFT;
                    }
                    if (splitWords[x] == "FROM")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["FROM"] = NODE_LEFT;
                    }
                    if (splitWords[x] == "WHERE")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["WHERE"] = NODE_LEFT;
                    }
                    if (splitWords[x] == "INSERT")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["INSERT"] = NODE_LEFT;
                    }

                    if (splitWords[x] == "INTO")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["INTO"] = NODE_LEFT;
                    }
                    if (splitWords[x] == "CREATE")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["CREATE"] = NODE_LEFT;
                    }
                    if (splitWords[x] == "TABLE")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["TABLE"] = NODE_LEFT;
                    }
                }
            }

            // Check if a pattern of words have been fufilled
            if (keyWords["INSERT"] is string && keyWords["INTO"] is string)
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                insertToTable(DB_NAME, keyWords, splitWords);
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                sys.send("Completed in " + elapsedMs + " milliseconds. \n");
            }
            else if (keyWords["CREATE"] is string && keyWords["TABLE"] is string)
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                createNewTable(splitWords, DB_NAME, keyWords["TABLE"]);
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                sys.send("Completed in " + elapsedMs + "milliseconds. \n");
            }
            else if (keyWords["SELECT"] is string && keyWords["FROM"] is string)
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                searchEngine(splitWords, DB_NAME, keyWords["SELECT"], keyWords["FROM"], keyWords["WHERE"]);
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                sys.send("Completed in " + elapsedMs + "milliseconds. \n");
            }
        }
        public string returnMsg()
        {
            Console.WriteLine(res);
            return res;
        }
    }

    class Server
    {
        // Variables
        static IPAddress IP = IPAddress.Parse("127.0.0.1");
        static TcpListener SERVER = new TcpListener(IP, 16032);
        static String DB_NAME = "";
        static String DB_PASS = "";
        static String QUERY_ENGINE_PEFORMANCE = "STANDARD";

        static void Main(string[] args)
        {
            server_start();
            loadCommandLine();
        }
        // Starts server && Preview checks
        static void server_start()
        {
            previewSettingsCheck();
            var x = new setupDatabase();
            x.initaliseDatabase(DB_NAME);
            SERVER.Start();
            loadingMessage();
            acceptConnection();
        }

        // Activates TCP listener
        static async void acceptConnection()
        {
            while (true)
            {
                TcpClient client = await SERVER.AcceptTcpClientAsync();
                ThreadPool.QueueUserWorkItem(handle_connection, client);
            }
        }

        // Handling connection model, will be revamped to 16 threads model
        static async void handle_connection(object clientObj)
        {
            TcpClient client = (TcpClient)clientObj;
            int threadId = Thread.CurrentThread.ManagedThreadId;

            Console.WriteLine($"New connection\n User Thread Id:{threadId}");

            try
            {
                while (true)
                {
                    NetworkStream ns = client.GetStream();
                    var buffer = new byte[1024];

                    int received = await ns.ReadAsync(buffer);
                    var message = Encoding.UTF8.GetString(buffer, 0, received);
                    Console.WriteLine("Request:" + message);

                    QueryInputEngine e = new QueryInputEngine();
                    e.query(message, DB_NAME, true);
                    byte[] responseBytes = System.Text.Encoding.UTF8.GetBytes(e.returnMsg());

                    if (responseBytes.ToString() != "") await ns.WriteAsync(responseBytes);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Client disconnected");
            }
            finally
            {
                client.Close();
            }
        }

        static void loadingMessage()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Booting up NoahSQL server..");
            Console.WriteLine("Hosting...");
            Thread.Sleep(2000);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Succesfully hosted on " + (SERVER.LocalEndpoint).ToString() + "\n");
            Console.ResetColor();
        }

        static void loadCommandLine()
        {

            while (true)
            {
                Console.WriteLine("Command: ");
                string command = Console.ReadLine();

                if (command == "--details")
                {
                    Console.WriteLine("\nDatabase Name: " + DB_NAME);
                    Console.WriteLine("Database Pass: " + DB_PASS + "\n");
                }

                if (command.Contains("--execute"))
                {
                    Console.WriteLine("\n");
                    QueryInputEngine e = new QueryInputEngine();
                    e.query(command, DB_NAME, true);
                }
            }
        }
        // Checks for custom settings && applies them
        static void previewSettingsCheck()
        {
            if (File.Exists("settings.json"))
            {
                String content = File.ReadAllText("settings.json");
                settingsJson settings = JsonConvert.DeserializeObject<settingsJson>(content);

                DB_NAME = settings.username;
                DB_PASS = settings.password;
                QUERY_ENGINE_PEFORMANCE = settings.peformance;
            }

            if (!File.Exists("settings.json"))
            {
                Console.ForegroundColor = ConsoleColor.Blue;
                Console.WriteLine("Welcome to NoahSQL, user!");
                Console.WriteLine("You are currently using NoahSQL BETA 1.0");
                Console.WriteLine("Currently, the NoahSQL Query Engine is still being built");
                Console.WriteLine("\nLets get started, by creating a database: \n");
                Console.ResetColor();

                Console.ForegroundColor = ConsoleColor.DarkGray;
                Console.WriteLine("Enter DB Name >> ");
                DB_NAME = Console.ReadLine();
                Console.WriteLine("Enter DB Password >> ");
                DB_PASS = Console.ReadLine();

                Console.WriteLine("\nOk, now lets customize some settings [More settings will come soon]");

                Console.WriteLine("\nPerformance [LOW, STANDARD, HIGH] >> ");
                String QUERY_ = Console.ReadLine();

                if (QUERY_ == "LOW" || QUERY_ == "Low" || QUERY_ == "low")
                {
                    QUERY_ENGINE_PEFORMANCE = "LOW";
                }

                if (QUERY_ == "STANDARD" || QUERY_ == "Standard" || QUERY_ == "standard")
                {
                    QUERY_ENGINE_PEFORMANCE = "STANDARD";
                }

                if (QUERY_ == "HIGH" || QUERY_ == "High" || QUERY_ == "high")
                {
                    QUERY_ENGINE_PEFORMANCE = "HIGH";
                }

                Console.ResetColor();
                Console.WriteLine("\n");

                Sys Sys = new Sys();
                Sys.send("Set DB Name: " + DB_NAME);
                Sys.send("Set DB Password: " + DB_NAME);

                var x = new setupDatabase();
                x.initaliseDatabase(DB_NAME);

                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.WriteLine("\nSettings have been applied, thank you. Your NoahSQL server will now boot. Enjoy : )\n");
                Console.ResetColor();

                settingsJson setUser = new settingsJson();
                setUser.username = DB_NAME;
                setUser.password = DB_PASS;
                setUser.peformance = QUERY_ENGINE_PEFORMANCE;

                File.WriteAllTextAsync("settings.json", JsonConvert.SerializeObject(setUser));
            }
        }
    }

    // Used to package json
    public class settingsJson
    {
        public string username { get; set; }
        public string password { get; set; }
        public string peformance { get; set; }
    }

    public class row
    {
        public List<dynamic> values { get; set; }
    }

    public class column
    {
        public Dictionary<string, string> values = new Dictionary<string, string>();
        public List<int> id { get; set; }
    }
}
