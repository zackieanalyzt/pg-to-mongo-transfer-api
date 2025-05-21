const express = require('express');
const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const app = express();

// ตั้งค่าเชื่อมต่อ PostgreSQL
const pgPool = new Pool({
  user: 'postgres',
  host: '192.168.100.70',
  database: 'databank',
  password: 'grespost',
  port: 5432,
});

// ตั้งค่าเชื่อมต่อ MongoDB
const mongoUrl = 'mongodb://localhost:27017';
const dbName = 'migration_db';
let mongoClient;

(async () => {
  mongoClient = await MongoClient.connect(mongoUrl, {
    useUnifiedTopology: true,
  });
  console.log('Connected to MongoDB');
})();

// Endpoint สำหรับโอนย้ายข้อมูล
app.get('/migrate', async (req, res) => {
  const { year, month } = req.query;

  try {
    // ดึงข้อมูลจาก PostgreSQL
    const pgResult = await pgPool.query(
      'SELECT * FROM items WHERE year = $1 ',
      [year]
    );

    // แปลงข้อมูลให้พร้อมบันทึก
    const items = pgResult.rows.map(row => ({
      hn: row.hn,
      vn: row.vn,
      visit_type: row.visit_type,
      quantity: row.quantity,
      price: row.price,
      item_common_name: row.item_common_name,
      item_group: row.item_group,
      year: row.year,
      month: row.month,
      date: row.date,
      byear: row.byear,
      quarter: row.quarter
    }));

    // บันทึกข้อมูลลง MongoDB
    const db = mongoClient.db(dbName);
    const collection = db.collection('items');
    await collection.insertMany(items);

    res.json({ success: true, migrated: items.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// เริ่มเซิร์ฟเวอร์
app.listen(3000, () => {
  console.log('Server running on port 3000');
});

// เรียกดูข้อมูลจาก MongoDB
app.get('/get-items', async (req, res) => {
  try {
    const db = mongoClient.db('migration_db');
    const collection = db.collection('items');
    const year = parseInt(req.query.year) || 2019; // ใช้ค่าเริ่มต้นเป็น 2568 ถ้าไม่ระบุ
    const items = await collection.find({ year: year }).toArray();

    res.json({ success: true, data: items });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});