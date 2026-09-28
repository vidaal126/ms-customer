/*
  Warnings:

  - A unique constraint covering the columns `[sequence]` on the table `outbox_events` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "outbox_events" ADD COLUMN     "sequence" BIGSERIAL NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "outbox_events_sequence_key" ON "outbox_events"("sequence");
