      ******************************************************************
      * MIG 000001-1  -  ACCOUNT EXTRACT RECORD  (PROJECT1)
      *
      * Reference copybook for the fixed-width .DAT layout produced by
      * the Extractor. The machine-readable source of truth is
      * contracts/tds/tds-src-project1.def -- this file documents the
      * mainframe-side view the offsets were derived from.
      *
      * Total record: 59 bytes, all fixed-offset fields -- homogeneous per
      * definition (docs/PLAN-CHANGES-21082026.md D6), no JSON side-channel.
      ******************************************************************
       01  ACCOUNT-RECORD.
           05  ACCT-ID              PIC X(12).
           05  CUST-ID              PIC X(10).
           05  CLIENT-TYPE          PIC X(04).
           05  PRODUCT-CODE         PIC X(06).
           05  CURRENCY             PIC X(03).
           05  OPEN-DATE            PIC 9(08).
           05  STATUS               PIC X(01).
      *    Signed display numeric, two decimals: +00000001234.56 is carried
      *    as a 15-byte field = sign(1) + 11 digits + point(1) + 2 digits.
           05  BALANCE              PIC S9(11)V99 DISPLAY.
