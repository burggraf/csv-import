import { Component } from '@angular/core';
import Papa from 'papaparse'
import { createClient, SupabaseClient } from '@supabase/supabase-js';

import { analyzeRow, analyzeRowResults, detectType, determineWinner } from './importHelpers';

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
    processed: 0
  };

  constructor() {
    console.log(Papa);
    this.importSpec.SUPABASE_KEY = localStorage.getItem('SUPABASE_KEY');
    this.importSpec.SUPABASE_URL = localStorage.getItem('SUPABASE_URL');
  }

  async start() {
    this.timer = +new Date();
    this.importSpec.count = 0;
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
    let fieldNameArr = [];
    let fieldsArray = [];
    console.log('calling parse with quoteChar', importSpec.quoteChar);
    await Papa.parse(file, {
      download: false, // true,
      // quoteChar: importSpec.enclosedBy,
      header: true, //(importSpec.headerLine === '1'),
      skipEmptyLines: true,
      // dynamicTyping: true,
      quoteChar: importSpec.quoteChar,
      // LocalChunkSize: 1024 * 1024 * 0.25, // 100 MB
      // RemoteChunkSize: 1024 * 1024 * 100, // 100 MB
      // newline: '\r\n',
      // worker: true,
      chunk: async function(results, parser) {
        console.log(`***************************************`);
        console.log(`*** got chunk of ${results.data.length}`);
        console.log(`*** quoteChar: ${importSpec.quoteChar}`);
        console.log(`***************************************`);
        if (importSpec.abort) parser.abort();
        if (importSpec.ready) {
          parser.pause();
          /*
          console.log(`calling importCSV, ${results.data.length}`);
          let rows = results.data.splice(0, 5000);
          console.log(`rows: ${rows.length}, left: ${results.data.length}`);
          
          while (results.data.length > 0 && !importSpec.abort) {
            const { data, error } = await importCSV(importSpec, rows);
            rows = results.data.splice(0, 5000);
            if (error) {
              console.error('importCSV error', error);
            } else {
              console.log('importCSV success');
            }
            console.log(`rows: ${rows.length}, left: ${results.data.length}`);
          }
          */
          const { data, error } = await importCSV(importSpec, results.data);
          if (error) {
            console.error('importCSV error', error);
          } else {
            console.log('importCSV success');
          }
          console.log(`Records per sec: ${+((importSpec.processed / (+new Date() - timer)).toFixed(2))}`);
          console.log(`cursor ${results.meta.cursor} / ${(+new Date() - timer)}`);
          console.log(`Bytes per ms: ${+((results.meta.cursor / (+new Date() - timer)).toFixed(2))}`);
          parser.resume();
        } else {
          console.log("Row data.length:", results.data.length);
          console.log("Row errors.length:", results.errors.length);
          console.log('Chunk => Meta', results.meta);
          console.log('quoteChar', importSpec.quoteChar);
          importSpec.count += results.data.length; 
          if (!fieldNameArr.length) fieldNameArr = results.meta.fields;
          // console.log('parser', parser);
          results.data.map((row) => {
            if (rowCount > 0 || (importSpec.headerLine === '0')) analyzeRow(fieldsHash, row);
            rowCount++;
          });  
          if (results.errors.length > 0) {
            results.errors.map((error) => {
              if ((error.code === 'InvalidQuotes' || error.code === 'TooManyFields') && importSpec.quoteChar === '') {
                // try changing the quoteChar to a double-quote and start over
                // start over
                console.log('*****************************************');
                console.log('******** start over *********************');
                console.log('*****************************************');
                importSpec.quoteChar = '"';
                // importSpec.abort = true;
                parser.abort();
                // start();
                // return;
              } else {

              }
            });
            console.log('*** there are errors', results.errors);
          }
        }
      },
      complete: function() {
        console.log('complete!');
        console.log('fieldsHash', fieldsHash);
        console.log('fieldNames is now', importSpec.fieldNames);
        console.log('record count', importSpec.count);
        if (importSpec.ready) {
          console.log('READY -> complete function skipped, we should be done.');
          const totalTime = +new Date() - timer;
          console.log(`TOTAL TIME: ${totalTime}`);
          console.log(`Records per sec: ${+((importSpec.processed / (+new Date() - timer) * 1000).toFixed(2))}`);
          return;
        }
        const fieldsArray = analyzeRowResults(fieldsHash);
        console.log('fieldsArray', fieldsArray);
        
        
        console.log('** fieldNameArr', fieldNameArr);
        const assignedFieldNames = [];
        let DDL = `(`;
        for (let x = 0; x < fieldsArray.length; x++) {
          let fieldName = (fieldNameArr[x] || 'field').trim();
          if (assignedFieldNames.indexOf(fieldName) > -1) {
            let suffix = 1;
            while (assignedFieldNames.indexOf(fieldName + suffix) > -1) {
              suffix++;
            }
            // console.log('adding suffix', suffix, 'to fieldName', fieldName);
            fieldName += suffix;
          }
          DDL += `"${fieldName}" ${fieldsArray[x].type.toUpperCase()}`;
          assignedFieldNames.push(fieldName.trim());
          importSpec.fieldTypes.push(fieldsArray[x].type.toUpperCase());
          if (x < fieldsArray.length - 1) DDL += `, `;
        }
        DDL += `)`;        
        console.log('DDL', DDL);
        importSpec.DDL = `CREATE TABLE "${importSpec.destinationTable}" ${DDL}`;
        importSpec.fieldNames = assignedFieldNames.join(',');
        importSpec.status = 'analyzed';
        checkDestinationTable();
      }
    });
  }
  
  importCSV = async (importSpec, rows) => {
    if (!this.supabase) this.supabase = createClient(importSpec.SUPABASE_URL, importSpec.SUPABASE_KEY);

    console.log(`-> insert into ${importSpec.destinationTable}`);
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
      } else {
        console.log('************ check destination table:', tbl);
        const destinationCheckErrors = [];
        let index = 0;
        console.log('this.importSpec', this.importSpec);
        console.log('** this.importSpec.fieldNames', this.importSpec.fieldNames);
        
        this.importSpec.fieldNames.split(',').map(fld => {
          // console.log('checking fld', fld, tbl.properties);
          // console.log('tbl.properties', tbl.properties);
          if (tbl.properties[fld].format.toUpperCase() !==  
              this.importSpec.fieldTypes[index].toUpperCase()) {
                if (tbl.properties[fld].format.toUpperCase() === 'DOUBLE PRECISION' && 
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
    if (true || this.importSpec.destinationTable.trim().length === 0) {
      const fileElement: any = document.getElementById('files');
      const file = fileElement.files[0];
      console.log('file', file);
      let name = file.name;
      // remove extension from name
      name = name.substring(0, name.indexOf('.'));
      this.importSpec.destinationTable = name;
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
      processed: 0
    };    
    const fileElement: any = document.getElementById('files');
    let file = fileElement.files[0];
    console.log('reset: file', file);
    fileElement.value = null;
  }

}
