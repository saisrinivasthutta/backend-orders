const express = require("express");
const path = require("path");
const cors = require("cors");

const { open } = require("sqlite");
const sqlite3 = require("sqlite3").verbose();
const corsOptions = {
  origin: "http://localhost:3000", // Allow only requests from this origin
  methods: "GET,POST", // Allow only these methods
  allowedHeaders: ["Content-Type", "Authorization"], // Allow only these headers
};
const app = express();
app.use(cors(corsOptions));
app.use(express.json());

const dbPath = path.join(__dirname, "orders.db");

let db = null;

//connecting to the server
const initializeDBAndServer = async () => {
  try {
    db = new sqlite3.Database("./orders.db");
    app.listen(3000, () => {
      console.log("Server Running at http://localhost:3000/");
    });
  } catch (e) {
    //console.log(`DB Error: ${e.message}`);
    process.exit(1);
  }
};

initializeDBAndServer();

//creating tables if they aren't exist
// 1 - PendingSellerOrders -> contains orders of sellers
//2 - PendingBuyerOrders -> contains orders of buyers
// 3- CompletedOrderTable -> contains completed transactions
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS PendingSellerOrders (
      id INTEGER PRIMARY KEY,
      seller_price REAL NOT NULL,
      seller_qty INTEGER NOT NULL
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS PendingBuyerOrders (
      id INTEGER PRIMARY KEY,
      buyer_price REAL NOT NULL,
      buyer_qty INTEGER NOT NULL
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS CompletedOrderTable (
      id INTEGER PRIMARY KEY,
      price REAL NOT NULL,
      qty INTEGER NOT NULL
    );
  `);
});

//inserting a new row . If any row having same seller price then we adding this quantity to existing.
// Otherwise new row created
app.post("/seller-orders", (request, response) => {
  const { seller_price, seller_qty } = request.body;
  db.get(
    `select * from PendingSellerOrders where seller_price = ${seller_price};`,
    (err, row) => {
      console.log(row);
      if (row === undefined) {
        db.run(
          `
    INSERT INTO PendingSellerOrders (seller_price, seller_qty)
    VALUES ( ?, ?);
  `,
          [seller_price, seller_qty],
          (err) => {
            if (err) {
              response
                .status(500)
                .send({ message: "Error : Can't create Order" });
            } else {
              response.send({ message: "Seller Order created successfully" });
            }
          }
        );
      } else {
        const prevSellerQty = row.seller_qty;
        const id = row.id;
        db.run(
          `update PendingSellerOrders set seller_qty = ${
            prevSellerQty + seller_qty
          } where id = ${id};`
        );
      }
    }
  );
});

//inserting a new row . If any row having same seller price then we adding this quantity to existing.
// Otherwise new row created
app.post("/buyer-orders", (request, response) => {
  const { buyer_price, buyer_qty } = request.body;
  db.get(
    `select * from PendingBuyerOrders where buyer_price = ${buyer_price};`,
    (err, row) => {
      if (row === undefined) {
        db.run(
          `
    INSERT INTO PendingBuyerOrders (buyer_price, buyer_qty)
    VALUES ( ?, ?);
  `,
          [buyer_price, buyer_qty],
          (err) => {
            if (err) {
              response
                .status(500)
                .send({ message: "Error : Can't create Order" });
            } else {
              response.send({ message: "Buyer Order created successfully" });
            }
          }
        );
      } else {
        const prevBuyerQty = row.buyer_qty;
        const id = row.id;
        db.run(
          `update PendingBuyerOrders set buyer_qty = ${
            prevBuyerQty + buyer_qty
          } where id = ${id};`
        );
      }
    }
  );
});

//getting records of the seller orders
app.get("/seller-pending-orders", (req, res) => {
  db.all(
    `
    SELECT * FROM PendingSellerOrders;
  `,
    (err, rows) => {
      if (err) {
        res.status(500).send({ message: "Error fetching orders" });
      } else {
        res.send(rows);
      }
    }
  );
});

//getting records of the buyers orders
app.get("/buyer-pending-orders", (req, res) => {
  db.all(
    `
    SELECT * FROM PendingBuyerOrders;
  `,
    (err, rows) => {
      if (err) {
        res.status(500).send({ message: "Error fetching orders" });
      } else {
        res.send(rows);
      }
    }
  );
});

//getting records of the completed transactions
app.get("/completed-orders", (req, res) => {
  db.all(
    `
    SELECT * FROM CompletedOrderTable;
  `,
    (err, rows) => {
      if (err) {
        res.status(500).send({ message: "Error fetching orders" });
      } else {
        res.send(rows);
      }
    }
  );
});

//Matching buyer orders with seller orders
//Here I am using TRANSACTION , COMMIT and ROLLBACK . These ensures ACID properties
app.post("/match-orders", (req, res) => {
  const { buyer_price, buyer_qty } = req.body;
  const query = ` SELECT * FROM PendingSellerOrders WHERE seller_price = ${buyer_price};`;
  db.get(query, (err, row) => {
    if (row === undefined) {
      res.status(500).send({ message: "No Orders Matched" });
    } else {
      const { id, seller_price, seller_qty } = row;
      db.run("begin transaction;");
      try {
        if (buyer_qty === seller_qty) {
          db.run(
            `delete from PendingBuyerOrders where buyer_price = ${buyer_price};`
          );
          db.run(`delete from PendingSellerOrders where id = ${id};`);
          db.run(
            `insert into CompletedOrderTable (price, qty) values (${buyer_price},${buyer_qty})`
          );
        } else if (buyer_qty < seller_qty) {
          db.run(
            `delete from PendingBuyerOrders where buyer_price = ${buyer_price};`
          );
          db.run(
            `update PendingSellerOrders set seller_qty = ${
              seller_qty - buyer_qty
            }`
          );
          db.run(
            `insert into CompletedOrderTable (price, qty) values (${buyer_price},${
              seller_qty - buyer_qty
            })`
          );
        } else {
          db.run(`delete from PendingSellerOrders where id = ${id};`);
          db.run(
            `update PendingBuyerOrders set buyer_qty = ${
              buyer_qty - seller_qty
            }`
          );
          db.run(
            `insert into CompletedOrderTable (price, qty) values (${buyer_price},${
              buyer_qty - seller_qty
            })`
          );
        }
        res.send({ message: "Orders matched successfully" });
        db.run("COMMIT", (err) => {
          if (err) {
            console.error("Error committing transaction:", err.message);
          } else {
            console.log("Transaction completed successfully.");
          }
        });
      } catch (e) {
        db.run("ROLLBACK");
      }
    }
  });
});

app.get("/", (request, response) => {
  response.send("Hi");
});
