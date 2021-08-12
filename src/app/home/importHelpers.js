// write a function to test whether a string is a valid postgres timestamptz
export const isPostgresTimestamp = (str) => {
    if (typeof str !== 'string') return false;
    var match = str.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.\d+$/);
    if (match) {
      var year = match[1],
          month = match[2],
          day = match[3],
          hour = match[4],
          minute = match[5],
          second = match[6]
  
      if (year.length === 4 && month.length === 2 && day.length === 2 && hour.length === 2 && minute.length === 2 && second.length === 2) {
        return true;
      }
    }
    return false;
  }
  
  
  //write a function to test whether a string is a valid postgres datetime
export const isPostgresDateTime = (str) => {
    if (typeof str !== 'string') return false;
    var match = str.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
    if (match) {
      var year = match[1],
          month = match[2],
          day = match[3],
          hour = match[4],
          minute = match[5],
          second = match[6]
      if (year.length === 4 && month.length === 2 && day.length === 2 && hour.length === 2 && minute.length === 2 && second.length === 2) {
        return true;
      }
    }
    return false;
  }
  
  
  
export const isPostgresDate = (str) =>{
    if (typeof str !== 'string') return false;
    var match = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (match) {
      var year = match[1],
          month = match[2],
          day = match[3];
      if (year.length === 4 && month.length === 2 && day.length === 2) {
        return true;
      }
    }
    return false;
  }
  
  
  
export const analyzeRow = (fieldsHash, row) =>{
    for (let key in row) {
      const value = row[key]
      const field = fieldsHash[key] || (fieldsHash[key] = { typesFound: {}, sample: null, maxLength: 0, enabled: true })
  
      // Tally the presence of this field type
      const type = detectType(value)
      if (!field.typesFound[type]) field.typesFound[type] = 0
      field.typesFound[type]++
  
      // Save a sample record if there isn't one already (earlier rows might have an empty value)
      if (!field.sample && value) {
        field.sample = value
      }
  
      // Save the largest length
      field.maxLength = Math.max(field.maxLength, value!==null?value.length:0)
    }
  }
  
export const detectType = (sample) =>{
    if (typeof sample !== 'string' || sample === '') {
      return 'text'
    } else if (isPostgresTimestamp(sample) && +sample >= 31536000) { 
      return 'timestamp'
    } else if (isPostgresDate(sample)) {
      return 'date'
    } else if (isPostgresDateTime(sample)) {
      return 'datetime'
    } else if (!isNaN(sample) && sample.includes('.')) {
      return 'float'
    } else if (sample === '1' || sample === '0' || ['true', 'false'].includes(sample.toLowerCase())) {
      return 'boolean'
    } else if (!isNaN(sample)) {
        try {
          // 4,294,967,295
          // if (sample.length > 9 && parseInt(sample.substr(0,1),10) > 3) {
          if (parseInt(sample, 10) > 4294967295) {
            return 'bigint';
          } else {
            return 'integer';
          }  
        } catch (e) {
            return 'bigint';
        }
    } else if (sample.length > 255) {
      return 'text'
    } else {
      return 'text' // string
    }
  }
  
export const analyzeRowResults = (fieldsHash) => {
    let fieldsArray = []
    for (let key in fieldsHash) {
      const field = fieldsHash[key]
      // Determine which field type wins
      field.type = determineWinner(field.typesFound)
      field.machineName = key
      // field.machineName = slug(key, {
      //   replacement: '_',
      //   lower: true
      // })
      field.sourceName = key
      // If any null values encountered, set field nullable
      if (field.typesFound['null']) {
        field.nullable = true
      }
      fieldsArray.push(field)
    }
    return fieldsArray
  }
  
  /**
   *  Determine which type wins
   *  - timestamp could be int
   *  - integer could be float
   *  - everything could be string
   *  - if detect an int, don't check for timestamp anymore, only check for float or string
   *  - maybe this optimization can come later...
   */
export const determineWinner = (fieldTypes) =>{
    const keys = Object.keys(fieldTypes)
  
    if (keys.length === 1) {
      return keys[0]
    } else if (fieldTypes.text) {
      return 'text'
    } else if (fieldTypes.string) {
      return 'string'
    } else if (fieldTypes.float) {
      return 'float'
    } else if (fieldTypes.bigint) {
        return 'bigint'
    } else if (fieldTypes.integer) {
      return 'integer'
    } else if (fieldTypes.boolean) {
        return 'boolean'
    } else { // TODO: if keys.length > 1 then... what? always string? what about date + datetime?
      console.log('undetermined field type');
      console.log('keys', keys);
      console.log('fieldTypes', fieldTypes);
      return fieldTypes[0]
    }
  }
  
  