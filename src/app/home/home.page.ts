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
    ready: false,
    sourceURL: '',
    fieldNames: '',
    fieldTypes: [],
    headerLine: '1',
    destinationTable: '',
    DDL: '',
    status: 'ready',
    SUPABASE_URL: '',
    SUPABASE_KEY: ''
  };

  constructor() {
    console.log(Papa);
    this.importSpec.SUPABASE_KEY = localStorage.getItem('SUPABASE_KEY');
    this.importSpec.SUPABASE_URL = localStorage.getItem('SUPABASE_URL');
  }

  async start() {
    this.timer = +new Date();
    await this.analyzeFile(1024 * 1024 * 20);
    console.log('this.importSpec', this.importSpec);
  }

  analyzeFile = async (CHUNKSIZE) => {
    const fieldsHash = {};
    const importSpec: any = this.importSpec;
    const importCSV = this.importCSV;
    const timer = this.timer;
    const checkDestinationTable = this.checkDestinationTable;
    importSpec.status = 'analyzing';
    Papa.LocalChunkSize = CHUNKSIZE; // 1024 * 1024 * 10;	// 10 MB
    Papa.RemoteChunkSize = CHUNKSIZE; // 1024 * 1024 * 10;	// 5 MB
    const fileElement: any = document.getElementById('files');
    const file = fileElement.files[0];
    let rowCount = 0;
    let fieldNameArr = [];
    let fieldsArray = [];
    await Papa.parse(file, {
      download: false, // true,
      // quoteChar: importSpec.enclosedBy,
      header: true, //(importSpec.headerLine === '1'),
      skipEmptyLines: true,
      dynamicTyping: true,
      quoteChar: '"',
      // LocalChunkSize: 1024 * 1024 * 0.25, // 100 MB
      // RemoteChunkSize: 1024 * 1024 * 100, // 100 MB
      // newline: '\r\n',
      // worker: true,
      chunk: async function(results, parser) {
        console.log(`***************************************`);
        console.log(`*** got chunk of ${results.data.length}`);
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

          parser.resume();
        } else {
          console.log("Row data:", results.data);
          console.log("Row errors:", results.errors);
          console.log('Chunk => Meta', results.meta);  
          if (!fieldNameArr.length) fieldNameArr = results.meta.fields;
          // console.log('parser', parser);
          results.data.map((row) => {
            if (rowCount > 0 || (importSpec.headerLine === '0')) analyzeRow(fieldsHash, row);
            rowCount++;
          });  
        }
      },
      complete: function() {
        console.log('complete!');
        console.log('fieldsHash', fieldsHash);
        console.log('fieldNames is now', importSpec.fieldNames);
        if (importSpec.ready) {
          console.log('READY -> complete function skipped, we should be done.');
          const totalTime = +new Date() - timer;
          console.log(`TOTAL TIME: ${totalTime}`);
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

    const { data, error} = await this.supabase.from(importSpec.destinationTable)
    .insert(rows, {returning: 'minimal'});
    if (error) {
      console.log('importCSV error', error);
    } else {
      console.log('importCSV data', data);
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
          console.log('checking fld', fld);
          console.log('tbl.properties', tbl.properties);
          if (tbl.properties[fld].format.toUpperCase() !==  
              this.importSpec.fieldTypes[index].toUpperCase()) {
                destinationCheckErrors.push(
                  `Destination table field missing or wrong type: ${fld} ${this.importSpec.fieldTypes[index].toUpperCase()} vs. ${tbl.properties[fld].format.toUpperCase()}`);
          }
          index++;
        });
        if (destinationCheckErrors.length) {
          console.error('CANNOT IMPORT');
          destinationCheckErrors.map(e => console.error(e));
        } else {
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

}
