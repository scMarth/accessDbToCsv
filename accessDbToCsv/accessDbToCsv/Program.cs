using System;
using System.Data.OleDb;
using System.Data;

namespace accessDbToCsv
{
    class Program
    {
        static void Main(string[] args)
        {
            string accessDB = "";
            string csv = "";

            if (args.Length != 2)
            {
                usageExit();
            }else
            {
                accessDB = @args[0];
                csv = @args[1];
            }

            OleDbConnection connection = new OleDbConnection();

            connection.ConnectionString = @"Provider=Microsoft.ACE.OLEDB.12.0;"
                + @"Data Source=" + accessDB + ";";

            string mainTableName = "TABLE";

            Console.WriteLine("accessDB: " + accessDB);
            Console.WriteLine("csv: " + csv);

            try
            {
                connection.Open();
                Console.WriteLine("Connection opened.");
                DataSet ds = new DataSet();
                Console.WriteLine("Temporary Dataset created.");
                OleDbCommand cmd = new OleDbCommand("select * from [" + mainTableName + "] order by [ID] asc", connection);
                Console.WriteLine("SQL Query set.");
                OleDbDataAdapter da = new OleDbDataAdapter(cmd);
                Console.WriteLine("SQL Query passed.");

                Console.Write("Attempting to populate dataset...");
                da.Fill(ds);
                Console.WriteLine("Dataset populated.");

                auxFileWriterLib.createFile(csv);

                foreach (DataTable table in ds.Tables)
                {
                    // write header
                    for (int columnIndex = 0; columnIndex < table.Columns.Count; columnIndex++)
                    {
                        auxFileWriterLib.appendToFile(csv, table.Columns[columnIndex].ToString());
                        if (columnIndex != table.Columns.Count - 1) auxFileWriterLib.appendToFile(csv, ",");
                    }
                    auxFileWriterLib.appendToFile(csv, Environment.NewLine);

                    Console.WriteLine("{0} rows in table.", table.Rows.Count);
                    Console.WriteLine("Processing...");
                    for (int i=0; i < table.Rows.Count; i++)
                    {
                        DataRow row = table.Rows[i];

                        for (int j=0; j < table.Columns.Count; j++)
                        {
                            auxFileWriterLib.appendToFile(csv, row[j].ToString());
                            if (j != table.Columns.Count - 1) auxFileWriterLib.appendToFile(csv, ",");
                        }
                        auxFileWriterLib.appendToFile(csv, Environment.NewLine);
                    }

                }
            }catch (Exception)
            {
                Console.WriteLine("Failed to connect to data source.");
            }
            finally
            {
                connection.Close();
            }
            Console.WriteLine("Finished exporting CSV. Press any key to exit...");
            Console.ReadKey();
        }

        private static void usageExit()
        {
            Console.Error.WriteLine("Usage: accessDbToCsv input.db output.csv");
            Console.Error.WriteLine("Aborting.");
            System.Environment.Exit(1);
        }
    }
}
