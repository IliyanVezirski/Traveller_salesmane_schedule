import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const root = path.resolve(".");
const dataDir = path.join(root, "data");
await fs.mkdir(dataDir, { recursive: true });

const headers = [
  "client_id",
  "client_name",
  "sales_rep",
  "lat",
  "lon",
  "visit_frequency",
  "fixed_weekday",
  "forbidden_weekdays",
  "preferred_weekdays",
  "cluster_manual",
  "notes",
];

const zones = [
  { name: "Mladost", lat: 42.6505, lon: 23.3791 },
  { name: "Lozenets", lat: 42.6728, lon: 23.3191 },
  { name: "Lyulin", lat: 42.7165, lon: 23.2567 },
  { name: "Nadezhda", lat: 42.7382, lon: 23.3042 },
];

function frequencyForIndex(index) {
  if (index <= 10) return 2;
  if (index <= 18) return 4;
  return 8;
}

function buildRowsForRep(repName, repCode, repOffset) {
  const rows = [];
  for (let i = 1; i <= 20; i += 1) {
    const zone = zones[(i - 1) % zones.length];
    const ring = Math.floor((i - 1) / zones.length);
    const lat = zone.lat + repOffset + ring * 0.0012 + (i % 2 ? 0.0004 : -0.0003);
    const lon = zone.lon + repOffset * 0.7 + ring * 0.0011 + (i % 3 ? 0.0005 : -0.0004);
    rows.push([
      `${repCode}-${String(i).padStart(3, "0")}`,
      `${zone.name} Client ${i}`,
      repName,
      Number(lat.toFixed(6)),
      Number(lon.toFixed(6)),
      frequencyForIndex(i),
      "",
      "",
      "",
      zone.name,
      "Smoke-test sample row",
    ]);
  }
  return rows;
}

async function saveWorkbook(filePath, sheetName, rows) {
  const workbook = Workbook.create();
  const sheet = workbook.worksheets.add(sheetName);
  sheet.getRangeByIndexes(0, 0, 1, headers.length).values = [headers];
  if (rows.length) {
    sheet.getRangeByIndexes(1, 0, rows.length, headers.length).values = rows;
  }
  sheet.getRangeByIndexes(0, 0, 1, headers.length).format = {
    fill: "#1F4E79",
    font: { bold: true, color: "#FFFFFF" },
  };
  sheet.freezePanes.freezeRows(1);
  sheet.getRangeByIndexes(0, 0, Math.max(2, rows.length + 1), headers.length).format.wrapText = false;

  const xlsx = await SpreadsheetFile.exportXlsx(workbook);
  await xlsx.save(filePath);
}

const sampleRows = [
  ...buildRowsForRep("Rep A", "RA", 0),
  ...buildRowsForRep("Rep B", "RB", 0.006),
];

const templateRows = [[
  "CLIENT-001",
  "Example Client",
  "Rep A",
  42.6977,
  23.3219,
  4,
  "",
  "",
  "",
  "",
  "Optional notes",
]];

await saveWorkbook(path.join(dataDir, "sample_clients.xlsx"), "Clients", sampleRows);
await saveWorkbook(path.join(dataDir, "input_clients_template.xlsx"), "Clients", templateRows);
