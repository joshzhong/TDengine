#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "os.h"
#include "osString.h"
#include "ttype.h"
#include "taos.h"
#include "tsclient.h"
#include "tscLog.h"
#include "hash.h"
#include "tskiplist.h"
#include "tscUtil.h"

typedef enum {
  LP_ITEM_TAG,
  LP_ITEM_FIELD
} LPItemKind;

typedef struct  {
  SStrToken key;
  SStrToken value;

  char name[TSDB_COL_NAME_LEN];
  int8_t type;
  int16_t bytes;

  char* payload;
}SLPItem;

typedef struct {
  SStrToken measToken;
  SStrToken tsToken;

  char sTableName[TSDB_TABLE_NAME_LEN];
  SArray* tags;
  SArray* fields;
  int64_t ts;

} SLPPoint;

typedef enum {
  LP_MEASUREMENT,
  LP_TAG_KEY,
  LP_TAG_VALUE,
  LP_FIELD_KEY,
  LP_FIELD_VALUE
} LPPart;

int32_t scanToCommaOrSpace(SStrToken s, int32_t start, int32_t* index, LPPart part) {
  for (int32_t i = start; i < s.n; ++i) {
    if (s.z[i] == ',' || s.z[i] == ' ') {
      *index = i;
      return 0;
    }
  }
  return -1;
}

int32_t scanToEqual(SStrToken s, int32_t start, int32_t* index) {
  for (int32_t i = start; i < s.n; ++i) {
    if (s.z[i] == '=') {
      *index = i;
      return 0;
    }
  }
  return -1;
}

int32_t setPointMeasurement(SLPPoint* point, SStrToken token) {
  point->measToken = token;
  if (point->measToken.n < TSDB_TABLE_NAME_LEN) {
    strncpy(point->sTableName, point->measToken.z, point->measToken.n);
    point->sTableName[point->measToken.n] = '\0';
  }
  return 0;
}

int32_t setItemKey(SLPItem* item, SStrToken key, LPPart part) {
  item->key = key;
  if (item->key.n < TSDB_COL_NAME_LEN) {
    strncpy(item->name, item->key.z, item->key.n);
    item->name[item->key.n] = '\0';
  }
  return 0;
}

int32_t setItemValue(SLPItem* item, SStrToken value, LPPart part) {
  item->value = value;
  return 0;
}

int32_t parseItemValue(SLPItem* item, LPItemKind kind) {
  char* sv = item->value.z;
  char* last = item->value.z + item->value.n - 1;

  if (isdigit(sv[0]) || sv[0] == '-') {
    if (*last == 'i') {
      item->type = TSDB_DATA_TYPE_BIGINT;
      item->bytes = (int16_t)tDataTypes[item->type].bytes;
      item->payload = malloc(item->bytes);
      char* endptr = NULL;
      *(item->payload) = strtoll(sv, &endptr, 10);
    } else {
      item->type = TSDB_DATA_TYPE_DOUBLE;
      item->bytes = (int16_t)tDataTypes[item->type].bytes;
      item->payload = malloc(item->bytes);
      char* endptr = NULL;
      *(item->payload) = strtold(sv, &endptr);
    }
  } else if ((sv[0] == 'L' && sv[1] =='"') || sv[0] == '"' ) {
    if (sv[0] == 'L') {
      item->type = TSDB_DATA_TYPE_NCHAR;
      uint32_t len = item->value.n - 3;
      char* ucs = malloc(len*TSDB_NCHAR_SIZE);
      int32_t ncharBytes = 0;
      taosMbsToUcs4(sv+2, len, ucs, len, &ncharBytes);
      item->bytes = VARSTR_HEADER_SIZE + ncharBytes;
      item->payload = malloc(VARSTR_HEADER_SIZE + ncharBytes);
      varDataSetLen(item->payload, ncharBytes);
      memcpy(varDataVal(item->payload), ucs, ncharBytes);
      free(ucs);
    } else if (sv[0]=='"'){
      item->type = TSDB_DATA_TYPE_BINARY;
      uint32_t bytes = item->value.n - 2;
      item->bytes = VARSTR_HEADER_SIZE + bytes;
      item->payload = malloc(VARSTR_HEADER_SIZE + bytes);
      varDataSetLen(item->payload, bytes);
      memcpy(varDataVal(item->payload), sv+1, bytes);
    }
  } else if (sv[0] == 't' || sv[0] == 'f' || sv[0]=='T' || sv[0] == 'F') {
    item->type = TSDB_DATA_TYPE_BOOL;
    item->bytes = tDataTypes[item->type].bytes;
    item->payload = malloc(tDataTypes[item->type].bytes);
    *(item->payload) = tolower(sv[0])=='t' ? true : false;
  }
  return 0;
}

int32_t compareLPItemKey(const void* p1, const void* p2) {
  const SLPItem* t1 = p1;
  const SLPItem* t2 = p2;
  uint32_t min = (t1->key.n < t2->key.n) ? t1->key.n : t2->key.n;
  int res = strncmp(t1->key.z, t2->key.z, min);
  if (res != 0) {
    return res;
  } else {
    return (int)(t1->key.n) - (int)(t2->key.n);
  }
}

int32_t setPointTimeStamp(SLPPoint* point, SStrToken tsToken) {
  point->tsToken = tsToken;
  return 0;
}

int32_t parsePointTime(SLPPoint* point) {
  if (point->tsToken.n <= 0) {
    point->ts = taosGetTimestampNs();
  } else {
    char* endptr = NULL;
    point->ts = strtoll(point->tsToken.z, &endptr, 10);
  }
  return 0;
}

int32_t tscParseLine(SStrToken line, SLPPoint* point) {
  int32_t pos = 0;

  int32_t start = 0;
  int32_t err = scanToCommaOrSpace(line, start, &pos, LP_MEASUREMENT);
  if (err != 0) {
    tscError("a");
    return err;
  }

  SStrToken measurement = {.z = line.z+start, .n = pos-start};
  setPointMeasurement(point, measurement);
  point->tags = taosArrayInit(64, sizeof(SLPItem));
  start = pos + 1;
  while (line.z[start] == ',') {
    SLPItem item;

    err = scanToEqual(line, start, &pos);
    if (err != 0) {
      tscError("b");
      goto error;
    }

    SStrToken tagKey = {.z = line.z + start, .n = pos-start};
    setItemKey(&item, tagKey, LP_TAG_KEY);

    start = pos + 1;
    err = scanToCommaOrSpace(line, start, &pos, LP_TAG_VALUE);
    if (err != 0) {
      tscError("c");
      goto error;
    }

    SStrToken tagValue = {.z = line.z + start, .n = pos-start};
    setItemValue(&item, tagValue, LP_TAG_VALUE);

    parseItemValue(&item, LP_ITEM_TAG);
    taosArrayPush(point->tags, &item);

    start = pos + 1;
  }

  taosArraySort(point->tags, compareLPItemKey);

  point->fields = taosArrayInit(64, sizeof(SLPItem));
  do {
    SLPItem item;
    err = scanToEqual(line, start, &pos);
    if (err != 0) {
      goto error;
    }
    SStrToken fieldKey = {.z = line.z + start, .n = pos- start};
    setItemKey(&item, fieldKey, LP_FIELD_KEY);

    start = pos + 1;
    err = scanToCommaOrSpace(line, start, &pos, LP_FIELD_VALUE);
    if (err != 0) {
      goto error;
    }
    SStrToken fieldValue = {.z = line.z + start, .n = pos - start};
    setItemValue(&item, fieldValue, LP_TAG_VALUE);

    parseItemValue(&item, LP_ITEM_FIELD);
    taosArrayPush(point->fields, &item);

    start = pos + 1;
  } while (line.z[pos] == ',');

  taosArraySort(point->fields, compareLPItemKey);

  SStrToken tsToken = {.z = line.z+start, .n = line.n-start};
  setPointTimeStamp(point, tsToken);
  parsePointTime(point);

  goto done;

error:
  // free array
  return err;
done:
  return 0;
}


int32_t tscParseLines(char* lines[], int numLines, SArray* points, SArray* failedLines) {
  for (int32_t i = 0; i < numLines; ++i) {
    SStrToken tkLine = {.z = lines[i], .n = strlen(lines[i])+1};
    SLPPoint point;
    tscParseLine(tkLine, &point);
    taosArrayPush(points, &point);
  }
  return 0;
}

TAOS_RES* taos_insert_by_lines(TAOS* taos, char* lines[], int numLines) {
  SArray* points = taosArrayInit(numLines, sizeof(SLPPoint));
  tscParseLines(lines, numLines, points, NULL);

  size_t numPoint = taosArrayGetSize(points);
  SHashObj* fieldName2Def = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  for (int i = 0; i < numPoint; ++i) {
    SLPPoint* point = taosArrayGet(points, i);
    size_t tagNum = taosArrayGetSize(point->tags);
    size_t fieldNum = taosArrayGetSize(point->fields);

    for (int j = 0; j < tagNum; ++j) {
      SLPItem* item = taosArrayGet(point->tags, j);
      char stableFieldKey[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
      int keyLen = snprintf(stableFieldKey, TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN, "%s.%s", point->sTableName, item->name);

      TAOS_FIELD* field = taosHashGet(fieldName2Def, stableFieldKey, keyLen);
      if (field != NULL) {

      }
      taosHashPut(fieldName2Def, stableFieldKey, keyLen, field, sizeof(TAOS_FIELD));
    }

    for (int j = 0; j < fieldNum; ++j) {
      SLPItem* item = taosArrayGet(point->tags, j);
      char stableFieldKey[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
      int keyLen = snprintf(stableFieldKey, TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN, "%s.%s", point->sTableName, item->name);

      TAOS_FIELD* field = taosHashGet(fieldName2Def, stableFieldKey, keyLen);
      if (field != NULL) {

      }
      taosHashPut(fieldName2Def, stableFieldKey, keyLen, field, sizeof(TAOS_FIELD));
    }
  }
  return NULL;
}

