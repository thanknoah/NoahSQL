// Imports
using Newtonsoft.Json;
using System;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.IO;
using System.Linq;

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
        static String res;
        static String DBName;
        static String tableName;
        static String table;

        public static async void searchEngine(string[] splitWords, string SELECT, string WHERE)
        {
            // Variables
            List<String> selectStatements = new List<String>();
            string firstOperation = null;
            dynamic secondOperation = null;
            int firstOperationElement = 0;

            // Checking if table exists
            if (File.Exists(table))
            {
                try
                {
                    // Check if it is single select statement && Check for multiple select statements
                    if (!SELECT.Contains(",")) selectStatements.Add(SELECT);
                    
                    for (int x = 0; x < splitWords.Length; x++)
                    {
                        if (SELECT.Contains(","))
                        {
                            if (splitWords[x].Contains(",")) selectStatements.Add(splitWords[x].Replace(",", ""));
                            if (splitWords[x] == "FROM") selectStatements.Add(splitWords[x - 1]);
                        }
                        if (WHERE is string)
                        {
                            if (x == splitWords.Length - 1) secondOperation = convertToType(splitWords[x], null);
                            if (x == splitWords.Length - 3) firstOperation = splitWords[x];
                        }
                    }

                    // Read file
                    using (StreamReader reader = new StreamReader(table))
                    {
                        string line = await reader.ReadLineAsync();
                        column preDefinedValues = JsonConvert.DeserializeObject<column>(line);
                        Dictionary<int, string> elementsToSearch = new Dictionary<int, string>();

                        // Getting elements to search in each line of database
                        foreach (string select in selectStatements)
                        {
                            int i = 0;
                            foreach (string preDefined in preDefinedValues.values.Keys)
                            {
                                if (select == preDefined) elementsToSearch.Add(i, select);
                                if (preDefined == firstOperation) firstOperationElement = i;
                                i++;
                            }
                        }

                        // Skip the predefined values && prepare response message
                        line = await reader.ReadLineAsync();
                        List<Dictionary<string, dynamic>> responseJson = new List<Dictionary<string, dynamic>>();

                        // Reading row by row to check for values
                        while ((line = await reader.ReadLineAsync()) != null)
                        {
                            row values = JsonConvert.DeserializeObject<row>(line);
                            List<int> indexToSearch = new List<int>();

                            // If where condition
                            if (firstOperation is string && secondOperation != null)
                            {
                                if (System.Object.ReferenceEquals(values.values[firstOperationElement], secondOperation))
                                {
                                    Dictionary<string, dynamic> lineJson = new Dictionary<string, dynamic>();
                                    foreach (int id in elementsToSearch.Keys)
                                    {
                                        lineJson.Add(elementsToSearch[id], values.values[id]);
                                    }
                                    responseJson.Add(lineJson);
                                }
                            }
                            else
                            {
                                Dictionary<string, dynamic> lineJson = new Dictionary<string, dynamic>();
                                foreach (int id in elementsToSearch.Keys)
                                {
                                    lineJson.Add(elementsToSearch[id], values.values[id]);
                                }
                                responseJson.Add(lineJson);
                            }
                        }

                        // Converting to string
                        if (!isError) res = JsonConvert.SerializeObject(responseJson);
                    }
                }
                catch (Exception e)
                {
                    sys.send($"Error retrieving {SELECT} from table {tableName}");
                    sys.send(e.ToString());
                    res = $"Error retrieving {SELECT} from table {tableName}";

                    isError = true;
                }
            }
            else
            {
                sys.send($"Table {tableName} doesnt exists");
                res = $"Table {tableName} doesnt exists";
            }
        }

        public static void createNewTable(string[] splitWords)
        {
            // Variables
            JsonSerializer packagedValue = new JsonSerializer();
            Dictionary<string, string> preDefinedValues = new Dictionary<string, string>();
            List<int> IDs = new List<int>();
            column json = new column();
            
            int i = 0;

            // Checking if table exists
            if (!File.Exists(table))
            {
                try
                {
                    for (int x = 2; x < splitWords.Length; x++)
                    {
                        if (splitWords[x].Contains("int") || splitWords[x].Contains("str"))
                        {
                            // Filter / Clean out characters
                            splitWords[x] = splitWords[x].Replace(",", ""); splitWords[x + 1] = splitWords[x + 1].Replace(",", "");
                            splitWords[x] = splitWords[x].Replace("(", ""); splitWords[x + 1] = splitWords[x + 1].Replace(")", "");
                            splitWords[x] = splitWords[x].Replace(")", ""); splitWords[x + 1] = splitWords[x + 1].Replace(")", "");

                            preDefinedValues.Add(splitWords[x + 1], splitWords[x]);
                            IDs.Add(i);

                            i++;
                        }
                    }

                    // Prepare JSON in package and insert the values into the newly created table
                    json.values = preDefinedValues;
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


        public static void insertToTable(Dictionary<string, string> keyWords, string[] splitWords)
        {
            // List of variables
            List<dynamic> valuesToBeAdded = new List<dynamic>();

            if (File.Exists(table))
            {
                using (StreamWriter file = File.AppendText(table))
                {
                    for (int x = 1; x < splitWords.Length-2; x++)
                    {
                        // Cleaning value, and converting it to right value
                        if (splitWords[x].Contains(",") || splitWords[x].Contains("(") || splitWords[x].Contains(")"))
                        {
                            splitWords[x] = splitWords[x].Replace(",", "");
                            splitWords[x] = splitWords[x].Replace("(", "");
                            splitWords[x] = splitWords[x].Replace(")", "");

                            dynamic convertedValue = convertToType(splitWords[x], null);
                            valuesToBeAdded.Add(convertedValue);

                            if (splitWords[x].Contains(")")) break;
                        }
                    }

                    // Packaging value and sending it to table
                    if (!isError)
                    {
                        row json = new row();
                        json.values = valuesToBeAdded;
                        var packagedValue = JsonConvert.SerializeObject(json);

                        file.WriteLine(packagedValue);
                        file.Close();
                        valuesToBeAdded.Clear();

                        sys.send("Added values to table " + keyWords["INTO"] + "\n");
                        res = "Added values to table " + keyWords["INTO"] + "\n";
                    }
                    else
                    {
                        valuesToBeAdded.Clear();
                        file.Close();
                    }
                }
            }
            else
            {
                sys.send(keyWords["INTO"] + " is not a valid table.\n");
                res = keyWords["INTO"] + " is not a valid table.\n";

                Console.WriteLine(table);
            }
        }
        public static dynamic convertToType(string value, dynamic returnValue)
        {
            // Converts to type
            Sys Sys = new Sys();
            String firstReplace = "";
            String secondReplace = "";

            if (value.Contains("'"))
            {
                firstReplace = "'";
                secondReplace = "'";
            }
            else if (value.Contains("\""))
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
                    int convertedToInt = int.Parse(value);
                    return convertedToInt;
                }
                else
                {
                    value = value.Replace(firstReplace, "");
                    value = value.Replace(secondReplace, "");
                    return value;
                }
            }
            catch (Exception e)
            {
                Sys.send($"There was an error with uploading value {value} to table {tableName}. Current supported formats: INT (1,2,3,4) OR STR (\"\" OR '')");
                res = $"There was an error with uploading value {value} to table {tableName}. Current supported formats: INT (1,2,3) OR STR (\"\" OR '')";
                isError = true;
                return null;
            }

        }
        public void query(string query, bool distributedFiles, string DB_name)
        {
            // Lexer
            Dictionary<string, string> keyWords = new Dictionary<string, string>();
            query = query.Replace("--execute", "");
            string[] splitWords = query.Split(' ');

            DBName = DB_name;

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
                        tableName = keyWords["FROM"];
                        table = System.IO.Directory.GetCurrentDirectory() + @"\" + DBName + @"\" + tableName + ".json";
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
                        tableName = keyWords["INTO"];
                        table = System.IO.Directory.GetCurrentDirectory() + @"\" + DBName + @"\" + tableName + ".json";
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
                        tableName = keyWords["TABLE"];
                        table = System.IO.Directory.GetCurrentDirectory() + @"\" + DBName + @"\" + tableName + ".json";
                    }
                }
            }

            // Check if a pattern of words have been fufilled
            if (keyWords["INSERT"] is string && keyWords["INTO"] is string)
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                insertToTable(keyWords, splitWords);
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                sys.send("Completed in " + elapsedMs + " milliseconds. \n");
            }
            else if (keyWords["CREATE"] is string && keyWords["TABLE"] is string)
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                createNewTable(splitWords);
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                sys.send("Completed in " + elapsedMs + "milliseconds. \n");
            }
            else if (keyWords["SELECT"] is string && keyWords["FROM"] is string)
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                searchEngine(splitWords, keyWords["SELECT"], keyWords["WHERE"]);
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                sys.send("Completed in " + elapsedMs + "milliseconds. \n");
            }
        }
        public string returnMsg()
        {
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

                    QueryInputEngine e = new QueryInputEngine();
                    e.query(message, true, DB_NAME);
                    string result = await Task.Run(() => e.returnMsg());

                    byte[] responseBytes = System.Text.Encoding.UTF8.GetBytes(result);
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
                    e.query(command, true, DB_NAME);
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
