# csv-import
Proof of concept CSV import to PostgreSQL using Papaparse and Supabase.js

## UI Components
- File chooser button
- Destination table name (input box)
  - defaults the the filename of the chosen file without extension but user can override this
- Quote Chararacter (input box)
  - defaults to a double-quote for CSV files, no quote character for TXT or TSV (tab-delimited files)
- Abort button (for aborting the import process)
- Reset button (for resetting this UI)
- Input boxes for SUPABASE_URL and SUPABASE_KEY (not necessary if you already have these values available)
- Text Area (used to display the DDL for creating the table so the user can cut and paste it into a query window -- you can get rid of this if you create the table automatically)

## Main Code
- start() resets all counters and starts the analyze process
- analyzeFile(CHUNKSIZE) first pass analyzes the file, determines field names, etc.
  - called with importSpec.ready === false, analyzes the file
  - called with importSpec.ready === true, imports the file
- importCSV(importSpec, rows) imports a batch of data into the database
- checkDestinationTable() checks that the destination table exists and it contains the proper field types we need to import the data
- reset() resets the settings so we can import a different file

## importHelpers.js
- analyze each row to determine field types
- track field types and lengths used by each row
- determines what field type needs to be used

