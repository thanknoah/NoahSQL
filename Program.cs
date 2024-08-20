// Imports
using Newtonsoft.Json;
using System;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.IO;
using System.Linq;
using Microsoft.CSharp.RuntimeBinder;

namespace NoahSQL
{
    // Send notifications
    class Sys
    {
        public void send(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[SYSTEM]: {msg}");
            Console.ForegroundColor = ConsoleColor.Magenta;
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
        private static SemaphoreSlim semaphore = new SemaphoreSlim(1, 1000000);

        public static async void deleteFromTable(string[] splitWords, string WHERE)
        {
            // Variables
            string firstOperation = null;
            dynamic secondOperation = null;
            int firstOperationElement = 0;
            int index = 0;
            var watch = System.Diagnostics.Stopwatch.StartNew();

            // Checking if table exists
            if (File.Exists(table))
            {
                try
                {
                    // Checking for WHERE Statements
                    if (WHERE is string)
                    {
                        firstOperation = splitWords[splitWords.Length - 3];
                        secondOperation = convertToType(splitWords[splitWords.Length - 1], null);

                        Console.WriteLine(firstOperation, secondOperation);
                    }

                    // Reading file
                    await semaphore.WaitAsync();
                    string tempFilePath = Path.GetTempFileName();
                    using (StreamReader reader = new StreamReader(new FileStream(table, FileMode.Open, FileAccess.Read, FileShare.None)))
                    using (var writer = new StreamWriter(new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None)))
                    {
                        string line = await reader.ReadLineAsync();
                        column preDefinedValues = JsonConvert.DeserializeObject<column>(line);
                        Dictionary<int, string> elementsToSearch = new Dictionary<int, string>();

                        foreach (string preDefined in preDefinedValues.values.Keys)
                        {
                            if (preDefined == firstOperation) firstOperationElement = index;
                            index++;
                        }

                        writer.WriteLine(line);
                        line = await reader.ReadLineAsync();

                        // Reading row by row to check for values
                        while ((line = await reader.ReadLineAsync()) != null)
                        {
                            row values = JsonConvert.DeserializeObject<row>(line);
                            if (firstOperation is not string || secondOperation == null) break;
                            if (values.values[firstOperationElement] != secondOperation) writer.WriteLine(line);
                        }
                    }

                    File.Delete(table);
                    File.Move(tempFilePath, table);
                }
                catch (RuntimeBinderException ex)
                {
                    res = JsonConvert.SerializeObject("{}");
                }
                catch (Exception e)
                {
                    sys.send($"Error deleting from table {tableName}");
                    sys.send(e.ToString());
                    res = $"Error retrieving from table {tableName}";

                    isError = true;
                }
                finally
                {
                    semaphore.Release();
                    watch.Stop();
                    var elapsedMs = watch.ElapsedMilliseconds;
                    sys.send("Completed in " + elapsedMs + " milliseconds. \n");
                }

            } else
            {
                sys.send($"Table {tableName} doesnt exists");
                res = $"Table {tableName} doesnt exists";
            }
        }

        public static async void searchEngine(string[] splitWords, string SELECT, string WHERE)
        {
            // Variables
            List<String> selectStatements = new List<String>();
            string firstOperation = null;
            dynamic secondOperation = null;
            int firstOperationElement = 0;
            var watch = System.Diagnostics.Stopwatch.StartNew();

            // Checking if table exists
            if (File.Exists(table))
            {
                try
                {
                    // Check if it is single select || where statement
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
                        foreach (string select in selectStatements)
                        {
                            int index = 0;
                            foreach (string preDefined in preDefinedValues.values.Keys)
                            {
                                if (select == preDefined) elementsToSearch.Add(index, select);
                                if (preDefined == firstOperation) firstOperationElement = index;
                                index++;
                            }
                        }

                        // Skip the predefined values && prepare response message
                        line = await reader.ReadLineAsync();
                        List<Dictionary<string, dynamic>> responseJson = new List<Dictionary<string, dynamic>>();

                        // Reading row by row to check for values
                        while ((line = await reader.ReadLineAsync()) != null)
                        {
                            row values = JsonConvert.DeserializeObject<row>(line);
                            dynamic indivisualValue = values.values[firstOperationElement];

                            if (firstOperation is string && secondOperation != null)
                            {
                                if (indivisualValue == secondOperation)
                                {
                                    Dictionary<string, dynamic> lineJson = new Dictionary<string, dynamic>();
                                    foreach (int id in elementsToSearch.Keys)
                                    {
                                        lineJson.Add(elementsToSearch[id], indivisualValue);
                                    }
                                    responseJson.Add(lineJson);
                                }
                            }
                            else
                            {
                                Dictionary<string, dynamic> lineJson = new Dictionary<string, dynamic>();
                                foreach (int id in elementsToSearch.Keys)
                                {
                                    lineJson.Add(elementsToSearch[id], indivisualValue);
                                }
                                responseJson.Add(lineJson);
                            }
                        }

                        // Converting to string
                        if (!isError) res = JsonConvert.SerializeObject(responseJson);
                    }
                }
                catch (RuntimeBinderException ex)
                {
                    res = JsonConvert.SerializeObject("{}");
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

            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            sys.send("Completed in " + elapsedMs + "milliseconds. \n");
        }

        public static void createNewTable(string[] splitWords)
        {
            // Variables
            JsonSerializer packagedValue = new JsonSerializer();
            var watch = System.Diagnostics.Stopwatch.StartNew();
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
                    }
                    sys.send("Created Table " + tableName + " and inserted values.");
                    res = "Created Table " + tableName + " and inserted values. \n";
                }
            }
            else
            {
                sys.send("Table already exists\n");
                res = "Table already exists\n";
            }

            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            sys.send("Completed in " + elapsedMs + "milliseconds. \n");
        }


        public async static void insertToTable(Dictionary<string, string> keyWords, string[] splitWords)
        {
            // List of variables
            List<dynamic> valuesToBeAdded = new List<dynamic>();
            var watch = System.Diagnostics.Stopwatch.StartNew();

            if (File.Exists(table))
            {
                try
                {
                    using (StreamWriter file = new StreamWriter(table, append: true))
                    {
                        for (int x = 1; x < splitWords.Length - 2; x++)
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

                            await semaphore.WaitAsync();
                            await file.WriteLineAsync(packagedValue);
                            semaphore.Release();
                            valuesToBeAdded.Clear();

                            sys.send("Added values to table " + keyWords["INTO"]);
                            res = "Added values to table " + keyWords["INTO"] + "\n";
                        }
                        else
                        {
                            valuesToBeAdded.Clear();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (res == "")
                    {
                        sys.send("Error inserting values to database");
                        res = "Error inserting values to database";
                    }

                    semaphore.Release();
                }
                finally
                {
                    watch.Stop();
                    var elapsedMs = watch.ElapsedMilliseconds;
                    sys.send("Completed in " + elapsedMs + " milliseconds. \n");
                }
            }
            else
            {
                sys.send(keyWords["INTO"] + " is not a valid table");
                res = keyWords["INTO"] + " is not a valid table.\n";
            }
        }
        public static dynamic convertToType(String value, dynamic returnValue)
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
                Sys.send($"There was an error with converting {value} to table {tableName}. Current supported formats: INT (1,2,3,4) OR STR (\"\" OR '')");
                res = $"There was an error with converting {value} to table {tableName}. Current supported formats: INT (1,2,3) OR STR (\"\" OR '')";
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
            keyWords.Add("DELETE", null);
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
                    if (splitWords[x] == "DELETE")
                    {
                        string NODE_LEFT = splitWords[x + 1];
                        keyWords["DELETE"] = NODE_LEFT;
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
                insertToTable(keyWords, splitWords);
            }
            else if (keyWords["CREATE"] is string && keyWords["TABLE"] is string)
            {
                createNewTable(splitWords);                
            }
            else if (keyWords["SELECT"] is string && keyWords["FROM"] is string)
            {
                searchEngine(splitWords, keyWords["SELECT"], keyWords["WHERE"]);
            }
            else if (keyWords["DELETE"] is string && keyWords["FROM"] is string)
            {
                deleteFromTable(splitWords, keyWords["WHERE"]);
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
            try
            {
                while (true)
                {
                    TcpClient client = await SERVER.AcceptTcpClientAsync();
                    ThreadPool.QueueUserWorkItem(handle_connection, client);
                }
            } catch (Exception e)
            {
                Sys sys = new Sys();
                sys.send("There was an error accepting connections");
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
            Console.WriteLine("Booting up NoahSQL V1.8 BETA server..");
            Console.WriteLine("Hosting...");
            Thread.Sleep(2000);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Succesfully hosted on " + (SERVER.LocalEndpoint).ToString() + "\n");
            Console.ForegroundColor = ConsoleColor.Green;
            Sys sys = new Sys();
            sys.send("NOTE: If you need help with any commands, type --help\n");
            Console.ForegroundColor = ConsoleColor.Magenta;
        }

        static void loadCommandLine()
        {

            while (true)
            {
                Console.WriteLine("Command: ");
                string command = Console.ReadLine();

                if (command == "--help")
                {
                    Console.WriteLine("\nList of console commands: \n");
                    Console.WriteLine("--list (LISTS ALL TABLES)");
                    Console.WriteLine("--details (LISTS DB DETAILS)");
                    Console.WriteLine("--execute (EXECUTES SQL");
                    Console.WriteLine("\nList of NoahSQL commands: \n");
                    Console.WriteLine("CREATE TABLE [YOUR_TABLE_NAME]");
                    Console.WriteLine("INSERT (str [VAL1], int [VAL2]) INTO [YOUR_TABLE_NAME]");
                    Console.WriteLine("SELECT [VAL1], [VAL2] FROM [YOUR_TABLE_NAME]");
                    Console.WriteLine("SELECT [VAL1], [VAL2] FROM [YOUR_TABLE_NAME] WHERE [VAL] = [VAL]");
                    Console.WriteLine("DELETE FROM [YOUR_TABLE_NAME]");
                    Console.WriteLine("DELETE FROM [YOUR_TABLE_NAME] WHERE [VAL] = [VAL]");
                    Console.WriteLine("\n");
                }

                if (command == "--list")
                {
                    Console.WriteLine("\nList of tables: \n");

                    string DBDir = Directory.GetCurrentDirectory() + @"\" + DB_NAME;
                    string[] tableList = Directory.GetFiles(DBDir, "*.*", SearchOption.AllDirectories);

                    foreach (string tableDir in tableList)
                    {
                        string tableName = Path.GetFileName(tableDir);
                        string toRemove = ".json";
                        string result = tableName.Replace(toRemove, string.Empty);

                        Console.WriteLine(result);
                    }
                    Console.WriteLine("\n");
                }

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
                Console.WriteLine("You are currently using NoahSQL BETA V1.8");
                Console.WriteLine("Currently, this product is been finalised");
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
