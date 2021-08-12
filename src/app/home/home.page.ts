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
  private supabase: SupabaseClient;
  public importSpec = {  
    typ: [],
    fld: [],
    ready: false,
    sourceType: 'csv',
    sourceURL: '',
    delimiter: ',',
    enclosedBy: '',
    fieldNames: '',
    fieldTypes: [],
    headerLine: '1',
    domain: '',
    port: '',
    database: '',
    user: '',
    password: '',
    destinationTable: '',
    truncate: '0',
    isReady: false,
    ddl: '',
    DDL: '',
    jobId: '',
    jobName: '',
    status: 'ready',
    RESULTS: [],
    SUPABASE_URL: '',
    SUPABASE_KEY: ''
  };

  constructor() {
    console.log(Papa);
  }

  async start() {
    await this.analyzeFile();
    // console.log('this.importSpec.destinationTable', this.importSpec.destinationTable);
    // console.log('this.importSPec.ddl', this.importSpec.ddl);
    console.log('this.importSpec', this.importSpec);
    // console.log('this.DDL', this.DDL);
    // this.DDL = `CREATE TABLE ${this.importSpec.destinationTable} ` + this.DDL;
    // console.log('this.DDL', this.DDL);
  }

  analyzeFile = async () => {
    const fieldsHash = {};
    const importSpec: any = this.importSpec;
    const importCSV = this.importCSV;
    const checkDestinationTable = this.checkDestinationTable;
    importSpec.status = 'analyzing';
    Papa.LocalChunkSize = 1024 * 1024 * 5;	// 10 MB
    Papa.RemoteChunkSize = 1024 * 1024 * 5;	// 5 MB
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
      // LocalChunkSize: 1024 * 1024 * 0.25, // 100 MB
      // RemoteChunkSize: 1024 * 1024 * 100, // 100 MB
      // newline: '\r\n',
      // worker: true,
      chunk: function(results, parser) {
        if (importSpec.abort) parser.abort();
        console.log("Row data:", results.data);
        console.log("Row errors:", results.errors);
        console.log('Chunk => Meta', results.meta);
        if (importSpec.ready) {
          console.log('calling importCSV');
          importCSV(importSpec, results.data);
        } else {
          if (importSpec.fld.length === 0) {
            for (let f in results.data[0]) {
              importSpec.fld.push(f);
              importSpec.typ.push(typeof results.data[0][f]);
            }
          } else {
            for (let i = 0; i < importSpec.fld.length; i++) {
              if (typeof results.data[0][importSpec.fld[i]] !== importSpec.typ[i]) {
                console.error(`type error for field importSpec.fld[i]`);
              }
            }
          }
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
          return;
        }
        const fieldsArray = analyzeRowResults(fieldsHash);
        console.log('fieldsArray', fieldsArray);
        
        // let DDL = `create table "${getFileName(importSpec.sourceURL, true)}" (`;
        
        console.log('** fieldNameArr', fieldNameArr);
        importSpec.ddl = [];
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
          importSpec.ddl.push([`${fieldName}`,`${fieldsArray[x].type.toUpperCase()}`]);
          importSpec.fieldTypes.push(fieldsArray[x].type.toUpperCase());
          if (x < fieldsArray.length - 1) DDL += `, `;
        }
        DDL += `)`;        
        console.log('DDL', DDL);
        importSpec.DDL = `CREATE TABLE "${importSpec.destinationTable}" ${DDL}`;
        importSpec.fieldNames = assignedFieldNames.join(',');
        importSpec.status = 'analyzed';
        console.log(importSpec.fld, importSpec.typ);
        checkDestinationTable();
      }
    });
  }
  
  importCSV = async (importSpec, rows) => {
    if (!this.supabase) this.supabase = createClient(importSpec.SUPABASE_URL, importSpec.SUPABASE_KEY);
    console.log('ready to import rows', rows);
    const { data, error} = await this.supabase.from(importSpec.destinationTable)
    .insert(rows/*, {returning: 'minimal'}*/);
    if (error) {
      console.log('importCSV error', error);
    } else {
      console.log('importCSV data', data);
    }
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
        console.log('tbl', tbl);
        const destinationCheckErrors = [];
        let index = 0;
        this.importSpec.fld.map(fld => {
          if (tbl.properties[fld].format.toUpperCase() !==  
              this.importSpec.typ[index].toUpperCase()) {
                console.error(`Destination table field missing or wrong type: $\{fld}`);
          }
          index++;
        });
        if (destinationCheckErrors.length) {
          console.error('CANNOT IMPORT');
        } else {
          this.importSpec.ready = true;
          this.analyzeFile();
        }

      }
    }
    
  }

}
