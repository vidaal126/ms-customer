-- CreateTable
CREATE TABLE "customers" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "document" TEXT NOT NULL,
    "email" TEXT,
    "phone" TEXT,
    "authorizedTransportTypeIds" TEXT[],
    "createdAt" TIMESTAMP(3) NOT NULL,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "customers_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "transport_types_replica" (
    "transportTypeId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "active" BOOLEAN NOT NULL,
    "sourceEventId" TEXT NOT NULL,
    "sourceOccurredAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "transport_types_replica_pkey" PRIMARY KEY ("transportTypeId")
);

-- CreateTable
CREATE TABLE "processed_events" (
    "eventId" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_events_pkey" PRIMARY KEY ("eventId")
);

-- CreateTable
CREATE TABLE "outbox_events" (
    "id" TEXT NOT NULL,
    "aggregateId" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "schemaVersion" INTEGER NOT NULL,
    "correlationId" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "publishedAt" TIMESTAMP(3),

    CONSTRAINT "outbox_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "idempotency_keys" (
    "key" TEXT NOT NULL,
    "requestHash" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "responseStatus" INTEGER,
    "responseBody" JSONB,
    "lockedAt" TIMESTAMP(3) NOT NULL,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "idempotency_keys_pkey" PRIMARY KEY ("key")
);

-- CreateIndex
CREATE UNIQUE INDEX "customers_document_key" ON "customers"("document");

-- CreateIndex
CREATE INDEX "customers_createdAt_id_idx" ON "customers"("createdAt", "id");

-- CreateIndex
CREATE INDEX "outbox_events_publishedAt_createdAt_idx" ON "outbox_events"("publishedAt", "createdAt");

-- CreateIndex
CREATE INDEX "idempotency_keys_expiresAt_idx" ON "idempotency_keys"("expiresAt");

-- CHECK constraints: escritos a mao (o Prisma nao os modela), espelhando o
-- dominio. A lista de transportes vai no CHECK (e nao em SET NOT NULL) para
-- nao divergir do modelo Prisma de lista escalar; unicidade dos ids fica no
-- dominio (CHECK nao aceita subconsulta).
ALTER TABLE "customers"
  ADD CONSTRAINT "customers_name_length" CHECK (char_length("name") BETWEEN 1 AND 200 AND "name" = btrim("name")),
  ADD CONSTRAINT "customers_document_cpf_format" CHECK ("document" ~ '^\d{3}\.\d{3}\.\d{3}-\d{2}$'),
  ADD CONSTRAINT "customers_email_normalized" CHECK ("email" IS NULL OR ("email" = lower(btrim("email")) AND char_length("email") BETWEEN 3 AND 254)),
  ADD CONSTRAINT "customers_phone_format" CHECK ("phone" IS NULL OR "phone" ~ '^\(\d{2}\) \d{4,5}-\d{4}$'),
  ADD CONSTRAINT "customers_transport_types_valid" CHECK (
    "authorizedTransportTypeIds" IS NOT NULL
    AND cardinality("authorizedTransportTypeIds") <= 50
    AND array_position("authorizedTransportTypeIds", NULL) IS NULL
  ),
  ADD CONSTRAINT "customers_updated_after_created" CHECK ("updatedAt" >= "createdAt");

ALTER TABLE "transport_types_replica"
  ADD CONSTRAINT "transport_types_replica_name_length" CHECK (char_length("name") BETWEEN 1 AND 100);

ALTER TABLE "outbox_events"
  ADD CONSTRAINT "outbox_events_schema_version_positive" CHECK ("schemaVersion" > 0);

ALTER TABLE "idempotency_keys"
  ADD CONSTRAINT "idempotency_keys_key_length" CHECK (char_length("key") BETWEEN 1 AND 255),
  ADD CONSTRAINT "idempotency_keys_status_valid" CHECK ("status" IN ('in_progress', 'completed')),
  ADD CONSTRAINT "idempotency_keys_completed_has_response" CHECK (
    "status" <> 'completed' OR ("responseStatus" IS NOT NULL AND "responseBody" IS NOT NULL)
  ),
  ADD CONSTRAINT "idempotency_keys_expires_after_lock" CHECK ("expiresAt" > "lockedAt");
