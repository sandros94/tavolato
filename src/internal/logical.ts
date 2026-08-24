import type { Annotation, PhysicalKind } from "../types.ts";
import { LogicalTypeId } from "./format.ts";

const PHYSICAL_NAMES: Readonly<Record<PhysicalKind, string>> = {
  bool: "BOOLEAN",
  i32: "INT32",
  i64: "INT64",
  f32: "FLOAT",
  f64: "DOUBLE",
  bytes: "BYTE_ARRAY",
  fixed: "FIXED_LEN_BYTE_ARRAY",
};

function physicalName(physical: PhysicalKind, typeLength: number | undefined): string {
  return `${PHYSICAL_NAMES[physical]}${physical === "fixed" ? `(${typeLength})` : ""}`;
}

function requires(
  annotation: string,
  expected: string,
  physical: PhysicalKind,
  typeLength: number | undefined,
): string {
  return `annotates ${physicalName(physical, typeLength)} as ${annotation}, but ${annotation} requires ${expected}`;
}

/*
 * A strict lower decimal bound for log10(2). The omitted tail is smaller than
 * one unit at this scale, so integer products with the lower and upper bounds
 * certify the comparison without floating-point rounding. The exact BigInt
 * fallback is only reachable if those 200 decimal places still straddle the
 * requested integer precision.
 */
const LOG10_2_LOWER_DIGITS =
  "30102999566398119521373889472449302676818988146210854131042746112710818927442450948692725211818617204068447719143099537909476788113352350599969233370469557506450296425419340266181973431160294350118390";
const LOG10_2_SCALE = 10n ** BigInt(LOG10_2_LOWER_DIGITS.length);
const LOG10_2_LOWER = BigInt(LOG10_2_LOWER_DIGITS);

/** Whether `typeLength` signed bytes can represent every value of `precision`. */
function fixedDecimalCanHold(precision: number, typeLength: number): boolean {
  const bits = BigInt(typeLength) * 8n - 1n;
  const target = BigInt(precision) * LOG10_2_SCALE;
  const lower = bits * LOG10_2_LOWER;
  if (target <= lower) return true;

  const upper = bits * (LOG10_2_LOWER + 1n);
  if (target >= upper) return false;

  return 10n ** BigInt(precision) < 1n << bits;
}

/**
 * Reports a known logical annotation whose physical storage contradicts the
 * Parquet format. `none` has no contract, while `unknown` is deliberately left
 * claimable so a newer annotation is not mistaken for a malformed file.
 *
 * @internal
 */
export function logicalTypePhysicalProblem(
  annotation: Annotation,
  physical: PhysicalKind,
  typeLength: number | undefined,
): string | undefined {
  switch (annotation.kind) {
    case "none": {
      return undefined;
    }
    case "unknown": {
      switch (annotation.id) {
        case LogicalTypeId.MAP:
        case LogicalTypeId.LIST:
        case LogicalTypeId.VARIANT:
        case LogicalTypeId.FILE: {
          const name =
            annotation.id === LogicalTypeId.MAP
              ? "MAP"
              : annotation.id === LogicalTypeId.LIST
                ? "LIST"
                : annotation.id === LogicalTypeId.VARIANT
                  ? "VARIANT"
                  : "FILE";
          return `annotates ${physicalName(physical, typeLength)} as ${name}, but ${name} requires a group`;
        }
        case LogicalTypeId.INTERVAL: {
          return physical === "fixed" && typeLength === 12
            ? undefined
            : requires("INTERVAL", "FIXED_LEN_BYTE_ARRAY(12)", physical, typeLength);
        }
        case LogicalTypeId.GEOMETRY:
        case LogicalTypeId.GEOGRAPHY: {
          const name = annotation.id === LogicalTypeId.GEOMETRY ? "GEOMETRY" : "GEOGRAPHY";
          return physical === "bytes"
            ? undefined
            : requires(name, "BYTE_ARRAY", physical, typeLength);
        }
        default: {
          // UNKNOWN may annotate any primitive. Unnamed and later union members
          // remain claimable because this version cannot know their contracts.
          return undefined;
        }
      }
    }
    case "string":
    case "enum":
    case "json":
    case "bson": {
      return physical === "bytes"
        ? undefined
        : requires(annotation.kind.toUpperCase(), "BYTE_ARRAY", physical, typeLength);
    }
    case "uuid": {
      return physical === "fixed" && typeLength === 16
        ? undefined
        : requires("UUID", "FIXED_LEN_BYTE_ARRAY(16)", physical, typeLength);
    }
    case "date": {
      return physical === "i32" ? undefined : requires("DATE", "INT32", physical, typeLength);
    }
    case "float16": {
      return physical === "fixed" && typeLength === 2
        ? undefined
        : requires("FLOAT16", "FIXED_LEN_BYTE_ARRAY(2)", physical, typeLength);
    }
    case "time": {
      const expected = annotation.unit === "millis" ? "INT32" : "INT64";
      const valid = annotation.unit === "millis" ? physical === "i32" : physical === "i64";
      return valid
        ? undefined
        : requires(`TIME(${annotation.unit.toUpperCase()})`, expected, physical, typeLength);
    }
    case "timestamp": {
      return physical === "i64" ? undefined : requires("TIMESTAMP", "INT64", physical, typeLength);
    }
    case "integer": {
      const expected = annotation.bitWidth === 64 ? "INT64" : "INT32";
      const valid = annotation.bitWidth === 64 ? physical === "i64" : physical === "i32";
      return valid
        ? undefined
        : requires(`INTEGER(${annotation.bitWidth})`, expected, physical, typeLength);
    }
    case "decimal": {
      if (
        !Number.isInteger(annotation.precision) ||
        annotation.precision < 1 ||
        !Number.isInteger(annotation.scale) ||
        annotation.scale < 0 ||
        annotation.scale > annotation.precision
      ) {
        return `declares DECIMAL with precision ${annotation.precision} and scale ${annotation.scale}, but precision must be positive and scale must be from 0 to the precision`;
      }
      if (physical === "i32") {
        return annotation.precision <= 9
          ? undefined
          : `annotates INT32 as DECIMAL with precision ${annotation.precision}, but INT32 permits at most 9 digits`;
      }
      if (physical === "i64") {
        return annotation.precision <= 18
          ? undefined
          : `annotates INT64 as DECIMAL with precision ${annotation.precision}, but INT64 permits at most 18 digits`;
      }
      if (physical === "bytes") return undefined;
      if (physical === "fixed") {
        return fixedDecimalCanHold(annotation.precision, typeLength ?? 0)
          ? undefined
          : `annotates FIXED_LEN_BYTE_ARRAY(${typeLength}) as DECIMAL with precision ${annotation.precision}, but that width cannot hold ${annotation.precision} digits`;
      }
      return requires(
        "DECIMAL",
        "INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY",
        physical,
        typeLength,
      );
    }
  }
}
