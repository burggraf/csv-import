import { Component } from '@angular/core';
import Papa from 'papaparse'
import { createClient, SupabaseClient } from '@supabase/supabase-js';

import { analyzeRow, analyzeRowResults } from './importHelpers';

@Component({
  selector: 'app-home',
  templateUrl: 'home.page.html',
  styleUrls: ['home.page.scss'],
})
export class HomePage {
  public timer = 0;
  private supabase: SupabaseClient;
  public importSpec = {  
    abort: false,
    ready: false,
    sourceURL: '',
    fieldNames: '',
    fieldTypes: [],
    headerLine: '1',
    destinationTable: '',
    quoteChar: '',
    DDL: '',
    status: 'ready',
    SUPABASE_URL: '',
    SUPABASE_KEY: '',
    count: 0,
    processed: 0,
    errors: 0
  };

  constructor() {
    console.log(Papa);
    this.importSpec.SUPABASE_KEY = localStorage.getItem('SUPABASE_KEY');
    this.importSpec.SUPABASE_URL = localStorage.getItem('SUPABASE_URL');
  }

  async start() {
    this.timer = +new Date();
    this.importSpec.count = 0;
    this.importSpec.errors = 0;
    this.importSpec.processed = 0;
    this.importSpec.abort = false;
    await this.analyzeFile(1024 * 1024 * 20);
    console.log('this.importSpec', this.importSpec);
  }

  analyzeFile = async (CHUNKSIZE) => {
    const fieldsHash = {};
    const importSpec: any = this.importSpec;
    const importCSV = this.importCSV;
    if (importSpec.ready) {
      console.log(`*** analyze time: ${(+new Date() - this.timer)}`);
      this.timer = +new Date(); // restart timer
    }
    const timer = this.timer;
    const start = this.start;
    const checkDestinationTable = this.checkDestinationTable;
    importSpec.status = 'analyzing';
    Papa.LocalChunkSize = CHUNKSIZE; // 1024 * 1024 * 10;	// 10 MB
    Papa.RemoteChunkSize = CHUNKSIZE; // 1024 * 1024 * 10;	// 5 MB
    const fileElement: any = document.getElementById('files');
    const file = fileElement.files[0];
    let rowCount = 0;
    let fieldsArray = [];
    console.log('calling parse with quoteChar', importSpec.quoteChar);
    await Papa.parse(file, {
      download: false, // true,
      // quoteChar: importSpec.enclosedBy,
      header: true, //(importSpec.headerLine === '1'),
      transformHeader: importSpec.ready ? (header) => { return header; } : (header) => {
        // enclose fields names with quotes
        header = '"' + header.replace(/"/g,'') + '"';
        return header;
      },
      skipEmptyLines: true,
      quoteChar: importSpec.quoteChar,
      chunk: async function(results, parser) {
        if (importSpec.abort) parser.abort();
        if (importSpec.ready) { // do the actual data import / insert here
          parser.pause();
          const { data, error } = await importCSV(importSpec, results.data);
          if (error) {
            console.error('importCSV error', error);
          }
          console.log(`Records per sec: ${+((importSpec.processed / (+new Date() - timer) * 1000).toFixed(2))}`);
          console.log(`cursor ${results.meta.cursor} / ${(+new Date() - timer)}`);
          console.log(`Bytes per ms: ${+((results.meta.cursor / (+new Date() - timer)).toFixed(2))}`);
          parser.resume();
        } else { // analyze the file before importing
          console.log('**************************************');
          console.log("Row data.length:", results.data.length);
          console.log("Row errors.length:", results.errors.length);
          console.log('Chunk => Meta', results.meta);
          importSpec.count += results.data.length; 
          importSpec.errors += results.errors.length;
          results.data.map((row) => {
            if (rowCount > 0 || (importSpec.headerLine === '0')) analyzeRow(fieldsHash, row);
            rowCount++;
          });  
          if (results.errors.length > 0) {
            /*
            results.errors.map((error) => {
              if ((error.code === 'InvalidQuotes' || error.code === 'TooManyFields') && importSpec.quoteChar === '') {
              } else {
              }
            });
            */
            console.log(`*** there are ${results.errors.length} errors`);//, results.errors);
          }
        }
      },
      complete: function() {
        console.log('complete!');
        console.log('fieldsHash', fieldsHash);
        console.log('fieldNames is now', importSpec.fieldNames);
        console.log('record count', importSpec.count);
        if (importSpec.ready) { // import should be done here
          console.log('READY -> complete function skipped, we should be done.');
          const totalTime = +new Date() - timer;
          console.log(`TOTAL TIME: ${totalTime}`);
          console.log(`Records per sec: ${+((importSpec.processed / (+new Date() - timer) * 1000).toFixed(2))}`);
          return;
        } // still analyzing the file here
        const fieldsArray = analyzeRowResults(fieldsHash);
        console.log('**************************************************');
        console.log('fieldsArray', fieldsArray);
        console.log('**************************************************');
        const assignedFieldNames = [];
        let DDL = `(`;
        for (let x = 0; x < fieldsArray.length; x++) {
          let fieldName = fieldsArray[x].sourceName || 'field'.trim();
          if (assignedFieldNames.indexOf(fieldName) > -1) {
            let suffix = 1;
            while (assignedFieldNames.indexOf(fieldName + suffix) > -1) {
              suffix++;
            }
            fieldName += suffix;
          }
          DDL += `${fieldName} ${fieldsArray[x].type.toUpperCase()}`;
          assignedFieldNames.push(fieldName.trim());
          importSpec.fieldTypes.push(fieldsArray[x].type.toUpperCase());
          if (x < fieldsArray.length - 1) DDL += `,\n `;
        }
        DDL += `)`;        
        console.log('DDL', DDL);
        importSpec.DDL = `CREATE TABLE "${importSpec.destinationTable}"\n${DDL}`;
        importSpec.fieldNames = assignedFieldNames.join('\t');
        importSpec.status = 'analyzed';
        checkDestinationTable();
      }
    });
  }
  
  importCSV = async (importSpec, rows) => {
    if (!this.supabase) this.supabase = createClient(importSpec.SUPABASE_URL, importSpec.SUPABASE_KEY);

    console.log(`-> insert into ${importSpec.destinationTable}`);//, rows);
    // console.log('rows', rows);
    const { data, error} = await this.supabase.from(importSpec.destinationTable)
    .insert(rows, {returning: 'minimal'});
    if (error) {
      console.log('importCSV error', error);
    } else {
      importSpec.processed += rows.length;
      console.log(`processed ${importSpec.processed} / ${importSpec.count}`);
    }
    return { data, error };
  }

  checkDestinationTable = async () => {
    if (!this.importSpec.destinationTable) {
      console.error('destinationTable not set');
      return;
    }
    if (!this.supabase) this.supabase = createClient(this.importSpec.SUPABASE_URL, this.importSpec.SUPABASE_KEY);
    const { data, error} = await this.supabase.from(''/*this.importSpec.destinationTable*/)
    .select('*');
    // .single();
    if (error) console.error('error in checkDestinationTable', error);
    else {
      console.log('data', data);
      const definitions = (data as any).definitions;
      const tbl = definitions ? definitions[this.importSpec.destinationTable] : null;
      if (!tbl) {
        console.log('desination table is missing');
        console.log('count: ', this.importSpec.count);
        console.log('errors: ', this.importSpec.errors);
      } else {
        console.log('************ check destination table:', tbl);
        const destinationCheckErrors = [];
        let index = 0;
        console.log('this.importSpec', this.importSpec);
        console.log('** this.importSpec.fieldNames', this.importSpec.fieldNames);
        
        this.importSpec.fieldNames.split('\t').map(fld => {
          if (fld.length > 63) {
            destinationCheckErrors.push(`column names cannot be more than 63 characters: ${fld}`);
          }
        });
        this.importSpec.fieldNames.split('\t').map(fld => {
          fld = fld.replace(/"/g,'');
          // console.log('checking fld', fld, tbl.properties);
          // console.log('tbl.properties', tbl.properties);
          if (tbl.properties[fld].format.toUpperCase() !==  
              this.importSpec.fieldTypes[index].toUpperCase()) {
                if (tbl.properties[fld].format.toUpperCase() === 'NUMERIC' && 
                this.importSpec.fieldTypes[index].toUpperCase() === 'FLOAT') {
                  // postgres turns float into double precision
                } else {
                  destinationCheckErrors.push(
                    `Destination table field missing or wrong type: ${fld} ${this.importSpec.fieldTypes[index].toUpperCase()} vs. ${tbl.properties[fld].format.toUpperCase()}`);
                }
          }
          index++;
        });
        if (destinationCheckErrors.length) {
          console.error('CANNOT IMPORT');
          destinationCheckErrors.map(e => console.error(e));
        } else {
          // one more check -- does the destination table have data in it?
          console.log('this.importSpec',this.importSpec);
          console.log(`checking table ${this.importSpec.destinationTable} to see if it has data`);
          
          const { data: countdata, error: counterror, count } = await this.supabase.from(this.importSpec.destinationTable)
          .select('*', { head: true, count: 'exact' });
          console.log('data', countdata);
          console.log('error', counterror);
          console.log('count', count);
          
          console.log('ready to load...');
          this.importSpec.ready = true;
          this.analyzeFile(1024 * 1024 * 0.25);
        }

      }
    }
    
  }

  inputChange() {
    localStorage.setItem('SUPABASE_KEY', this.importSpec.SUPABASE_KEY);
    localStorage.setItem('SUPABASE_URL', this.importSpec.SUPABASE_URL);
  }
  fileInputChange() {
    console.log('** fileInputChange()');
    const fileElement: any = document.getElementById('files');
    const file = fileElement.files[0];
    console.log('file', file);
    let name = file.name;
    // get file extension
    const ext = name.substr(name.lastIndexOf('.') + 1).toLowerCase();
    // remove extension from name
    name = name.substring(0, name.indexOf('.'));
    this.importSpec.destinationTable = name;
    if (ext === 'csv') {
      this.importSpec.quoteChar = '"';
    } else {
      this.importSpec.quoteChar = '';
    }
  }
  reset() {
    this.importSpec = {  
      abort: false,
      ready: false,
      sourceURL: '',
      fieldNames: '',
      fieldTypes: [],
      headerLine: '1',
      destinationTable: '',
      quoteChar: '',
      DDL: '',
      status: 'ready',
      SUPABASE_URL: this.importSpec.SUPABASE_URL,
      SUPABASE_KEY: this.importSpec.SUPABASE_KEY,
      count: 0,
      processed: 0,
      errors: 0
    };    
    const fileElement: any = document.getElementById('files');
    let file = fileElement.files[0];
    console.log('reset: file', file);
    fileElement.value = null;
  }

}
