using System;
using System.Data.OleDb;
using System.Data;

class auxAccDbLib
{
    // Updates the table 'tableName' in the database specified by 'connection'
    // Sets all values in the column 'setColumn' to 'setString'
    // where values in 'whereColumn' match any of the values in 'matchValues'
    public static void updateDb(OleDbConnection connection, string tableName, string setColumn, string setString, string whereColumn, ref string[] matchValues)
    {
        string query = "update [" + tableName + "] set [" + setColumn + "]=@rb1 where ";
        for (int i = 0; i < matchValues.Length; i++)
        {
            if (i != 0) query += "or ";
            //query += "[" + whereColumn + "]=@rb" + (i + 2).ToString();
            query += "StrComp([" + whereColumn + "],@rb" + (i + 2).ToString() + ",0) = 0"; // Use StrComp for case sensitivity
            if (i != matchValues.Length - 1) query += " ";
        }

        OleDbCommand cmdUpdate = new OleDbCommand(query, connection);
        cmdUpdate.Parameters.Clear();
        cmdUpdate.CommandType = CommandType.Text;

        cmdUpdate.Parameters.AddWithValue("rb1", setString);

        for (int i = 0; i < matchValues.Length; i++)
        {
            cmdUpdate.Parameters.AddWithValue("rb" + (i + 2).ToString(), matchValues[i]);
        }

        Console.Write("Executing SQL update for [" + setColumn + "] ...");
        Console.WriteLine(" SQL executed. {0} lines affected.", cmdUpdate.ExecuteNonQuery());
        Console.WriteLine();
    }

    // Updates the table 'tableName' in the database specified by 'connection'
    // Adds a new column 'newColumnName' of type 'columnType'
    public static void addNewColumnToDb(OleDbConnection connection, string tableName, string newColumnName, string columnType)
    {
        string query = "alter table [" + tableName + "] add [" + newColumnName + "] " + columnType;
        OleDbCommand cmdUpdate = new OleDbCommand(query, connection);
        cmdUpdate.Parameters.Clear();
        cmdUpdate.CommandType = CommandType.Text;
        Console.Write("Adding column [" + newColumnName + "] ...");
        Console.WriteLine(" SQL executed. {0} lines affected.", cmdUpdate.ExecuteNonQuery());
        Console.WriteLine();
    }

    // Updates the table 'tableName' in the database specified by 'connection'
    // Changes the value where ID = 'ID' and column = 'ColumnName' to 'inputStr'
    public static void updateValueInColumnAtID(OleDbConnection connection, string tableName, string columnName, string inputStr, int ID)
    {
        string query = "update [" + tableName + "] set [" + columnName + "]=@rb1 where [ID]=@rb2";
        OleDbCommand cmdUpdate = new OleDbCommand(query, connection);
        cmdUpdate.Parameters.Clear();
        cmdUpdate.CommandType = CommandType.Text;
        cmdUpdate.Parameters.AddWithValue("rb1", inputStr);
        cmdUpdate.Parameters.AddWithValue("rb2", ID);
        cmdUpdate.ExecuteNonQuery();
    }
}
