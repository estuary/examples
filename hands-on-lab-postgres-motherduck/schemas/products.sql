CREATE TABLE "public"."products" (
  "id" int PRIMARY KEY,
  "name" varchar COMMENT 'faker.internet.userName()',
  "merchant_id" int NOT NULL COMMENT 'faker.datatype.number()',
  "price" int COMMENT 'faker.datatype.number()',
  "status" varchar COMMENT 'faker.datatype.boolean()',
  "created_at" timestamp DEFAULT (now())
);
