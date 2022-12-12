// See https://aka.ms/new-console-template for more information

using Npgsql;

// Database connection variable.
NpgsqlConnection connect = new NpgsqlConnection(
    "Server=localhost:5432;" +
    "Database=postgres;" +
    "User Id=root;" +
    "Password=123456");

try
{

    // Connect to database.
    connect.Open();

}
catch (Exception e)
{

    // Message confirming unsuccessful database connection.
    Console.WriteLine("Database connection unsuccessful.");

    // Stop program execution.
    System.Environment.Exit(1);

}

// File path.
string filePath = @"/Users/elizavetakabak/Projects/ImportingCSV/L/examples.csv";

// Check if the CSV file exists.
if (!File.Exists(filePath))
{

    // Message stating CSV file could not be located.
    Console.WriteLine("Could not locate the CSV file.");

    // Stop program execution.
    System.Environment.Exit(1);

}

try
{

    // Assign the CSV file to a reader object.
    using var reader = new StreamReader(filePath);

    // Test for headers.
    bool headers = true;

    // Data variables.
    string line;
    string[] currentRow;
    string sqlCsvInfo;
    NpgsqlCommand sqlInsert;
    int recordCount = 0;

    // Process the contents of the reader object.
    while ((line = reader.ReadLine()) != null)
    {

        // Remove double quotes from the line.
        line = line.Replace("\"", "");

        // Split the line at the commas.
        currentRow = line.Split(';');

        // Check for correct column headers if first row.
        if (headers == true)
        {

            if (currentRow[0] != "site" ||
                currentRow[1] != "drive" ||
                currentRow[2] != "path" ||
                currentRow[3] != "id" ||
                currentRow[4] != "name" ||
                currentRow[5] != "weburl" ||
                currentRow[6] != "folder" ||
                currentRow[7] != "link" ||
                currentRow[8] != "user" ||
                currentRow[9] != "accesslevel" ||
                currentRow[10] != "scope")
            {

                // Message stating incorrect CSV file headers.
                Console.WriteLine("Incorrect CSV file headers.");

                // Stop program execution.
                System.Environment.Exit(1);

            }
            else
            {

                headers = false;

            }

        }
        else
        {

            // Construct the insert statement.
            sqlCsvInfo = @"
                INSERT INTO examples 
                    (site,drivepath,name,weburl,folder,link,scope)
                VALUES 
                    (@site, @drivepath, @name, @weburl, @folder, @link, @scope)
            ";

            // Query text incorporated into SQL command.
            sqlInsert = new NpgsqlCommand(sqlCsvInfo, connect);

            // Bind the parameters to the query.
            sqlInsert.Parameters.AddWithValue("@site", currentRow[0]);
            sqlInsert.Parameters.AddWithValue("@drivepath", currentRow[1] + currentRow[2]);
            // sqlInsert.Parameters.AddWithValue("@path", currentRow[2]);
            sqlInsert.Parameters.AddWithValue("@name", currentRow[4]);
            sqlInsert.Parameters.AddWithValue("@weburl", currentRow[5]);
            sqlInsert.Parameters.AddWithValue("@folder", currentRow[6]);
            sqlInsert.Parameters.AddWithValue("@link", currentRow[7]);
            sqlInsert.Parameters.AddWithValue("@scope", currentRow[10]);
            sqlInsert.Prepare();

            // Execute SQL.
            sqlInsert.ExecuteNonQuery();

            // Increment the record count.
            recordCount += 1;

        }

    }

    // Provide feedback on the number of records added.
    if (recordCount == 0)
    {

        Console.WriteLine("No new csv records added.");

    }
    else if (recordCount == 1)
    {

        Console.WriteLine(recordCount + " csv record added.");

    }
    else
    {

        Console.WriteLine(recordCount + " csv records added.");

    }

}
catch (Exception e)
{

    // Confirm error adding csv information and exit.
    Console.WriteLine(e);
    Console.WriteLine("Error adding csv information.");
    System.Environment.Exit(1);

}
finally
{

    // Close the database connection.
    connect.Close();

}