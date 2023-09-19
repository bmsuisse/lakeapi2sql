use arrow::array::Array;
use arrow::array::BinaryArray;
use arrow::array::BooleanArray;
use arrow::array::Date32Array;
use arrow::array::Date64Array;
use arrow::array::Decimal128Array;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::Float16Array;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::array::LargeBinaryArray;
use arrow::array::LargeStringArray;
use arrow::array::StringArray;
use arrow::array::Time32SecondArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::borrow::Cow;
use std::fmt::Display;
use std::time::Duration as StdDuration;
use tiberius::numeric::Numeric;
use tiberius::time::time::Date;
use tiberius::time::time::PrimitiveDateTime;
use tiberius::time::time::Time;
use tiberius::ColumnData;
use tiberius::IntoSql;
use tiberius::ToSql;
use tiberius::TokenRow;
use time::Duration;

// These functions loop like a hack, because, well, there are probably a hack
fn to_datetime(val: Option<PrimitiveDateTime>) -> ColumnData<'static> {
    return match val.to_sql() {
        ColumnData::DateTime2(x) => ColumnData::DateTime2(x),
        _ => panic!("should be mapped"),
    };
}
fn to_date(val: Option<Date>) -> ColumnData<'static> {
    return match val.to_sql() {
        ColumnData::Date(x) => ColumnData::Date(x),
        _ => panic!("should be mapped"),
    };
}
fn to_time(val: Option<Time>) -> ColumnData<'static> {
    return match val.to_sql() {
        ColumnData::Time(x) => ColumnData::Time(x),
        _ => panic!("should be mapped"),
    };
}
#[derive(Debug)]
pub(crate) struct NotSupportedError {
    dtype: DataType,
}
impl Display for NotSupportedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Cannot use data type {}", self.dtype))
    }
}
impl std::error::Error for NotSupportedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

pub(crate) fn get_token_rows<'a>(
    batch: &'a RecordBatch,
    cols: &'a Vec<String>,
) -> Result<Vec<TokenRow<'a>>, Box<dyn std::error::Error + Send + Sync>> {
    let unix_min_date = Date::from_calendar_date(1970, tiberius::time::time::Month::January, 1)?;
    let unix_min: PrimitiveDateTime = unix_min_date.with_time(Time::from_hms(0, 0, 0)?);

    let rows = batch.num_rows();
    let mut token_rows: Vec<TokenRow> = vec![TokenRow::new(); rows.try_into()?];

    let batchcols = batch
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().to_string())
        .collect::<Vec<String>>();
    let colsnames = if cols.len() == 0 { &batchcols } else { cols };
    for colname in colsnames {
        let mightcol = batch.column_by_name(colname);
        if let None = mightcol {
            for rowindex in 0..rows {
                token_rows[rowindex].push(ColumnData::String(None));
            }
            continue;
        }
        let col = mightcol.unwrap();
        //For docs: col.data_type().to_physical_type()
        match col.data_type() {
            arrow::datatypes::DataType::Boolean => {
                let ba = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(val.into_sql());
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Int32 => {
                let ba = col.as_any().downcast_ref::<Int32Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I32(val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                let ba = col.as_any().downcast_ref::<StringArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::String(match val {
                        Some(vs) => Some(Cow::from(vs)),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                let ba = col.as_any().downcast_ref::<LargeStringArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::String(match val {
                        Some(vs) => Some(Cow::from(vs)),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Millisecond,
                None,
            ) => {
                let ba = col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => {
                            if vs < 0 {
                                None
                            } else {
                                Some(unix_min + StdDuration::from_millis(vs as u64))
                            }
                        }
                        None => None,
                    };
                    token_rows[rowindex].push(to_datetime(dt_val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            ) => {
                let ba = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => {
                            if vs < 0 {
                                None
                            } else {
                                Some(unix_min + StdDuration::from_micros(vs as u64))
                            }
                        }
                        None => None,
                    };
                    token_rows[rowindex].push(to_datetime(dt_val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Null => {
                for rowindex in 0..rows {
                    token_rows[rowindex].push(ColumnData::I32(None));
                }
            }
            arrow::datatypes::DataType::UInt8 => {
                let ba = col.as_any().downcast_ref::<UInt8Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::U8(val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Int8 => {
                let ba = col.as_any().downcast_ref::<Int8Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I16(match val {
                        Some(v) => Some(v as i16),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Int16 => {
                let ba = col.as_any().downcast_ref::<Int16Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I16(val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Int64 => {
                let ba = col.as_any().downcast_ref::<Int64Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::UInt16 => {
                let ba = col.as_any().downcast_ref::<UInt16Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I32(match val {
                        Some(x) => Some(x.clone().into()),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::UInt32 => {
                let ba = col.as_any().downcast_ref::<UInt32Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(match val {
                        Some(x) => Some(x.clone().into()),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::UInt64 => {
                let ba = col.as_any().downcast_ref::<UInt64Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(match val {
                        Some(x) => Some(x.clone() as i64),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Float16 => {
                let ba = col.as_any().downcast_ref::<Float16Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::F32(match val {
                        Some(x) => Some(x.to_f32()),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Float32 => {
                let ba = col.as_any().downcast_ref::<Float32Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::F32(val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Float64 => {
                let ba = col.as_any().downcast_ref::<Float64Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::F64(val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Date32 => {
                let ba = col.as_any().downcast_ref::<Date32Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => {
                            if vs < 0 {
                                None
                            } else {
                                Some(unix_min_date + Duration::days(vs as i64))
                            }
                        }
                        None => None,
                    };
                    token_rows[rowindex].push(to_date(dt_val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Date64 => {
                let ba = col.as_any().downcast_ref::<Date64Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => {
                            if vs < 0 {
                                None
                            } else {
                                Some(unix_min_date + Duration::days(vs))
                            }
                        }
                        None => None,
                    };
                    token_rows[rowindex].push(to_date(dt_val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Second) => {
                let ba = col.as_any().downcast_ref::<Time32SecondArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val: Option<Time> = match val {
                        Some(vs) => {
                            if vs < 0 {
                                None
                            } else {
                                Some(
                                    Time::from_hms(
                                        // TODO: Testing
                                        (vs / 60 / 60).try_into().unwrap(),
                                        ((vs / 60) % 60).try_into().unwrap(),
                                        (vs % 60).try_into().unwrap(),
                                    )
                                    .unwrap(),
                                )
                            }
                        }
                        None => None,
                    };
                    token_rows[rowindex].push(to_time(dt_val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                let ba = col.as_any().downcast_ref::<Time32SecondArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val: Option<Time> = match val {
                        Some(vs) => {
                            if vs < 0 {
                                None
                            } else {
                                Some(
                                    Time::from_hms_milli(
                                        (vs / 1000 / 60 / 60).try_into().unwrap(),
                                        ((vs / 1000 / 60) % 60).try_into().unwrap(),
                                        ((vs / 1000) % 60).try_into().unwrap(),
                                        (vs % 1000).try_into().unwrap(),
                                    )
                                    .unwrap(),
                                )
                            }
                        }

                        None => None,
                    };
                    token_rows[rowindex].push(to_time(dt_val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Binary => {
                let ba = col.as_any().downcast_ref::<BinaryArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Binary(val.map(|x| Cow::from(x))));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::LargeBinary => {
                let ba = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Binary(val.map(|x| Cow::from(x))));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::FixedSizeBinary(_) => {
                let ba = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Binary(val.map(|x| Cow::from(x))));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Decimal128(_, s) => {
                let ba = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let scale: u8 = s.clone().try_into()?;
                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Numeric(
                        val.map(|x| Numeric::new_with_scale(x, scale)),
                    ));
                    rowindex += 1;
                }
            }
            dt => return Err(Box::new(NotSupportedError { dtype: dt.clone() })), //other => panic!("Not supported {:?}", other),
        }
    }
    Ok(token_rows)
}
