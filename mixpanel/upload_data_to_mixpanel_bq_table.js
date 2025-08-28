const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const readline = require("readline");
const { BigQuery } = require("@google-cloud/bigquery");

const BATCH_SIZE = 1000;
const folderPath = "<Path of the downloaded folder>";

// BigQuery Client
const bigQuery = new BigQuery({
  keyFilename: "<Path to BQ Service Account File>",
});

const datasetId = "<dataset to be uploaded>";
const processedTableId = "<table to be uploaded>";

// const keysToMask = ["EMAIL", "PHONE", "PHONENUMBER", "MOBILENUMBER", "_USER_ID", "DISTINCT_ID", "TOKEN"];
const keysToMask = [];
let count = 0;

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function maskFirstSixCharacters(value, key) {
  if (typeof value !== "string" && typeof value !== "number") {
    return value;
  }
  if(key === "DISTINCT_ID"){
    const isValidNumber = /^\+91\d{10}$/.test(value) || /^\d{10}$/.test(value);
    if(!isValidNumber) return value;
  }
  const stringValue = String(value);
  if (stringValue.length <= 9) {
    return "*".repeat(stringValue.length);
  }
  return "*".repeat(9) + stringValue.slice(9);
}

// const allowedValues = [
//   'GoodScore: Web view launched','GoodScore: Home viewed'
//   ]

function sanitizeRecord(record) {
  const sanitizedRecord = {};
  for (const [key, value] of Object.entries(record)) {
    if (value == 'GoodScore: Raise dispute automation CRIF login security question success on BE') {
      continue;
    }
    if (value === null || value === undefined || value === "" || (Array.isArray(value) && value.length === 0) ||
        (typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0)) {
      continue;
    }

    const sanitizedKey = key.replace(/[^a-zA-Z0-9_]/g, "_").toUpperCase();
    if (sanitizedKey === "OPTION") {
      continue;
    }

    if (sanitizedKey.startsWith("APPSFLYER_") || sanitizedKey.startsWith("DIALOGID_")) {
        continue;
     }
 
    let sanitizedValue = value;
    const fieldType = inferFieldType(value);
    if (fieldType === "NOT_FOUND") {
      sanitizedValue = typeof value === "object" ? JSON.stringify(value) : String(value);
    }
    if (keysToMask.includes(sanitizedKey)) {
      sanitizedValue = maskFirstSixCharacters(value, sanitizedKey);
    }
    if (!sanitizedKey || sanitizedKey === "" || sanitizedKey.length <= 1) continue;
    sanitizedRecord[sanitizedKey] = sanitizedValue;
  }
  return sanitizedRecord;
}

async function insertDataWithSchemaUpdate(datasetId, tableId, data) {
  const sanitizedData = data.map(record => sanitizeRecord(record));
  const dataset = bigQuery.dataset(datasetId);
  const table = dataset.table(tableId);

  try {
    const [tableMetadata] = await table.getMetadata();
    const currentSchema = tableMetadata.schema.fields;
    const newFields = getNewFields(currentSchema, sanitizedData);

    if (newFields.length > 0) {
      let updatedSchema;
      if (currentSchema) {
        updatedSchema = [...currentSchema, ...newFields];
      } else {
        updatedSchema = newFields;
      }
      await table.setMetadata({ schema: { fields: updatedSchema } });
      await delay(60000);
      console.log("Schema updated with new fields.");
    }

    await table.insert(sanitizedData);
    count += sanitizedData.length;
    console.log("Data inserted successfully.");
  } catch (error) {
    console.error("Error inserting data:", error);
    if (error.errors) {
      error.errors.forEach((err, idx) => {
        console.error(`Error ${idx + 1}:`, err);
      });
    } else {
      console.error("General Error:", error.message);
    }
  }
}

function getNewFields(currentSchema, data) {
  const currentFieldNames = new Map(
    currentSchema?.map((field) => [field.name, { type: field.type, mode: field.mode }])
  );
  const newFields = [];

  data.forEach((record) => {
    for (const [key, value] of Object.entries(record)) {
      const fieldType = inferFieldType(value);
      const mode = getMode(value);

      if (!currentFieldNames.has(key)) {
        newFields.push({ name: key, type: fieldType, mode: mode });
        currentFieldNames.set(key, { type: fieldType, mode: mode });
      } else {
        const existingFieldType = currentFieldNames.get(key).type;
        if (existingFieldType !== fieldType) {
          record[key] = formatValue(value, existingFieldType);
        }
      }
    }
  });

  return newFields;
}

function getMode(value) {
  if (Array.isArray(value)) {
    return "REPEATED";
  }
  return "NULLABLE";
}

function inferFieldType(value) {
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "STRING";
    }
    switch (typeof value[0]) {
      case "string":
        return "STRING";
      case "number":
        return "FLOAT";
      case "boolean":
        return "BOOLEAN";
      default:
        return "NOT_FOUND";
    }
  }

  switch (typeof value) {
    case "string":
      return "STRING";
    case "number":
      return "FLOAT";
    case "boolean":
      return "BOOLEAN";
    default:
      return "NOT_FOUND";
  }
}

function formatValue(value, targetType) {
  if (value === null || value === undefined) return null;

  switch (targetType) {
    case "STRING":
      return String(value);
    case "BOOLEAN":
      return Boolean(value);
    case "FLOAT":
      return parseFloat(value) || null;
    default:
      return String(value);
  }
}

async function processFile(filePath) {
  let readStream = null;
  let unzipStream = null;
  let rl = null;

  try {
    const stats = await fs.promises.stat(filePath);
    if (stats.size === 0) {
      console.log("File is empty");
      return;
    }

    readStream = fs.createReadStream(filePath);
    unzipStream = zlib.createGunzip();
    readStream.pipe(unzipStream);

    rl = readline.createInterface({
      input: unzipStream,
      crlfDelay: Infinity,
    });

    let batch = [];
    let lineCount = 0;
    let emptyLines = 0;
    let promises = [];

    for await (const line of rl) {
      lineCount++;

      if (!line || line.trim().length === 0) {
        emptyLines++;
        if (emptyLines % 100 === 0) {
          console.warn(`Skipped ${emptyLines} empty lines so far. Last empty line at line ${lineCount}`);
        }
        continue;
      }

      let json;
      try {
        json = JSON.parse(line);
      } catch (parseError) {
        console.error(`Invalid JSON at line ${lineCount}:`, {
          line: line.slice(0, 200) + (line.length > 200 ? "..." : ""),
          error: parseError.message,
        });
        continue;
      }

      if (!json || typeof json !== "object") {
        console.error(`Invalid data structure at line ${lineCount}`);
        continue;
      }

      const transformedEvent = {
        event: json.event,
        ...json.properties,
      };

      // if(!allowedValues.includes(transformedEvent.event)){
      //   continue;
      // }
      batch.push(transformedEvent);

      // if (batch.length >= BATCH_SIZE) {
      //   if(lineCount<3244000){
      //     batch = [];
      //     continue;
      //   }
      //   await insertDataWithSchemaUpdate(datasetId, processedTableId, batch);

      // batch.push(transformedEvent);

      if (batch.length >= BATCH_SIZE) {
        promises.push(insertDataWithSchemaUpdate(datasetId, processedTableId, batch));
        console.log(`Processed and inserted ${batch.length} records (at line ${lineCount})`);
        batch = [];
      }

      if (promises.length >= 10) {
        await Promise.all(promises);
        promises = [];
      }
    }

    await Promise.all(promises);

    if (batch.length > 0) {
      await insertDataWithSchemaUpdate(datasetId, processedTableId, batch);
      console.log(`Processed and inserted final ${batch.length} records`);
    }

    console.log(`File processing complete:
      - Total lines processed: ${lineCount}
      - Empty lines skipped: ${emptyLines}
      - Successfully processed: ${lineCount - emptyLines} lines`);
  } catch (err) {
    console.error("Fatal error processing file:", err);
  } finally {
    if (rl) rl.close();
    if (readStream) readStream.destroy();
    if (unzipStream) unzipStream.destroy();
  }
}

async function processFolder(folderPath) {
  try {
    const files = fs
      .readdirSync(folderPath)
      .filter((file) => file.endsWith(".gz"));
    console.log(`Found ${files.length} files to process.`);

    for (const file of files) {
      const filePath = path.join(folderPath, file);
      console.log(`Processing file: ${file}`);
      await processFile(filePath);
    }

    console.log("Total events pushed to BigQuery:", count);
    console.log("All files processed.");
  } catch (err) {
    console.error("Error processing folder:", err);
  }
}

// Start processing
processFolder(folderPath);
