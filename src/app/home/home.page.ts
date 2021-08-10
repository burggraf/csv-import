import { Component } from '@angular/core';
import Papa from 'papaparse'

import { analyzeRow, analyzeRowResults, detectType, determineWinner } from './importHelpers';

@Component({
  selector: 'app-home',
  templateUrl: 'home.page.html',
  styleUrls: ['home.page.scss'],
})
export class HomePage {

  public importSpec = {  
    sourceType: 'csv',
    sourceURL: '',
    delimiter: ',',
    enclosedBy: '',
    fieldNames: '',
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
    importSpec.status = 'analyzing';
    Papa.LocalChunkSize = 1024 * 1024 * 10;	// 10 MB
    Papa.RemoteChunkSize = 1024 * 1024 * 10;	// 5 MB
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
      // LocalChunkSize: 1024 * 1024 * 0.25, // 100 MB
      // RemoteChunkSize: 1024 * 1024 * 100, // 100 MB
      // newline: '\r\n',
      // worker: true,
      chunk: function(results, parser) {
        if (importSpec.abort) parser.abort();
        console.log("Row data:", results.data);
        console.log("Row errors:", results.errors);
        console.log('Meta', results.meta);
        if (!fieldNameArr.length) fieldNameArr = results.meta.fields;
        // console.log('parser', parser);
        results.data.map((row) => {
          if (rowCount > 0 || (importSpec.headerLine === '0')) analyzeRow(fieldsHash, row);
          rowCount++;
        });
      },
      complete: function() {
        console.log('complete!');
        console.log('fieldsHash', fieldsHash);
        console.log('fieldNames is now', importSpec.fieldNames);
        const fieldsArray = analyzeRowResults(fieldsHash);
        console.log('fieldsArray', fieldsArray);
        
        // let DDL = `create table "${getFileName(importSpec.sourceURL, true)}" (`;
        
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
          if (x < fieldsArray.length - 1) DDL += `, `;
        }
        DDL += `)`;        
        console.log('DDL', DDL);
        importSpec.DDL = `CREATE TABLE "${importSpec.destinationTable}" ${DDL}`;
        importSpec.fieldNames = assignedFieldNames.join(',');
        importSpec.status = 'analyzed';
        

      }
      /*
      complete: function(results, file) {
        console.log('results', results);
        const errors = results.errors;
        let rowCount = 0;
        results.data.map((row) => {
          if (rowCount > 0 || (importSpec.headerLine === '0')) analyzeRow(fieldsHash, row);
          rowCount++;
        });
  
        const fieldsArray = analyzeRowResults(fieldsHash);
  
        if (results?.errors?.length) {
          console.error('Parse Errors', results.errors);
        } 
        const delimiter = results?.meta?.delimiter;
        const linebreak = results?.meta?.linebreak;
        if (linebreak === '\r') {
          errors.unshift('files with a line terminator of \\r are not currently supported');
        }
        rowCount = results?.data?.length;
        importSpec.RESULTS = results;
        
        console.log('delimiter', delimiter.replace('\t','\\t').replace('\n','\\n').replace('\r','\\r'));
        console.log('linebreak', linebreak.replace('\t','\\t').replace('\n','\\n').replace('\r','\\r'));
        console.log('importSpec.headerLine', importSpec.headerLine);
        if (importSpec.headerLine === '1') {
          console.log('headers', results?.data[0]?.join(','));
        } else {
          console.log('no headers');
        }
        console.log('rowCount', rowCount);
        importSpec.delimiter = delimiter.replace('\t','\\t').replace('\n','\\n').replace('\r','\\r');
        console.log('headerline looks like:', results?.data[0]);
        if (importSpec.headerLine === '1') {
          importSpec.fieldNames = results?.data[0]?.join(',');
          console.log('fieldNames is now', importSpec.fieldNames);
        } else {
          importSpec.fieldNames = '';
          for (let i = 0; i < fieldsArray.length; i++) {
            importSpec.fieldNames += `,field${i+1}`;
          }
          importSpec.fieldNames = importSpec.fieldNames.substring(1);
        }
        console.log('fieldNames is now', importSpec.fieldNames);
        const fieldNameArr = importSpec.fieldNames.split(',');
        console.log('fieldNames is now', importSpec.fieldNames);
        
        // let DDL = `create table "${getFileName(importSpec.sourceURL, true)}" (`;
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
          if (x < fieldsArray.length - 1) DDL += `, `;
        }
        DDL += `)`;        
        console.log('DDL', DDL);
        importSpec.DDL = `CREATE TABLE "${importSpec.destinationTable}" ${DDL}`;
        importSpec.fieldNames = assignedFieldNames.join(',');
        importSpec.status = 'analyzed';
      }
      */
    });
  }
  
  importCSV = async (importSpec) => {

  }

}
