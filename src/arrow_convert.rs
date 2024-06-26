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
use arrow::array::TimestampNanosecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;

use arrow::record_batch::RecordBatch;
use rust_decimal::prelude::*;
use std::borrow::Cow;

use std::time::Duration as StdDuration;
use tiberius::numeric::Numeric;
use tiberius::time::time::Date;
use tiberius::time::time::PrimitiveDateTime;
use tiberius::time::time::Time;
use tiberius::ColumnData;
use tiberius::ColumnType;
use tiberius::ToSql;
use tiberius::TokenRow;

use crate::error::LakeApi2SqlError;

fn to_col_dt<'a>(d: ColumnData<'_>) -> ColumnData<'a> {
    match d {
        ColumnData::DateTime2(dt) => ColumnData::DateTime2(dt),
        ColumnData::DateTime(dt) => ColumnData::DateTime(dt),
        ColumnData::Time(dt) => ColumnData::Time(dt),
        _ => panic!("Not a time field"),
    }
}

pub(crate) fn get_token_rows<'a, 'b>(
    batch: &'a RecordBatch,
    colsnames: &'b Vec<(String, ColumnType)>,
) -> Result<Vec<TokenRow<'a>>, LakeApi2SqlError> {
    let unix_min_date =
        Date::from_calendar_date(1970, tiberius::time::time::Month::January, 1).unwrap();
    let sql_min_date =
        Date::from_calendar_date(1, tiberius::time::time::Month::January, 1).unwrap();
    let sql_min_datetime =
        Date::from_calendar_date(1900, tiberius::time::time::Month::January, 1).unwrap();
    let unix_min: PrimitiveDateTime = unix_min_date.with_time(Time::from_hms(0, 0, 0).unwrap());
    let sql_min_to_unix_min = (unix_min_date - sql_min_date).whole_days();
    let sql_min_dt_to_unix_min = (unix_min_date - sql_min_datetime).whole_days();
    let rows = batch.num_rows();
    //let mut token_rows: Vec<TokenRow> = vec![TokenRow::new(); rows.try_into()?];
    let mut token_rows: Vec<TokenRow<'a>> = Vec::with_capacity(rows);
    for _ in 0..rows {
        token_rows.push(TokenRow::with_capacity(colsnames.len()));
    }
    for (colname, coltype) in colsnames {
        let mightcol = batch.column_by_name(colname);

        if mightcol.is_none() {
            log::debug!("colname: {}. Not found", colname);
            for rowindex in 0..rows {
                token_rows[rowindex].push(ColumnData::String(None));
            }
            continue;
        }
        let col = mightcol.unwrap();
        log::debug!(
            "colname: {}. Dt: {:?}. Sql Type: {:?}",
            colname,
            col.data_type(),
            coltype
        );
        //For docs: col.data_type().to_physical_type()
        match col.data_type() {
            arrow::datatypes::DataType::Boolean => {
                assert!(coltype == &ColumnType::Bit || coltype == &ColumnType::Bitn);
                let ba = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Bit(val));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Int32 => {
                assert!(coltype == &ColumnType::Int4 || coltype == &ColumnType::Intn);
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
                    token_rows[rowindex].push(ColumnData::String(val.map(Cow::from)));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                let ba = col.as_any().downcast_ref::<LargeStringArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::String(val.map(Cow::from)));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                let ba = col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(match val {
                        Some(vs) => {
                            if vs < 0 {
                                ColumnData::DateTime2(None)
                            } else {
                                let pt = unix_min + StdDuration::from_millis(vs as u64);
                                to_col_dt(pt.to_sql())
                            }
                        }
                        None => ColumnData::DateTime2(None),
                    });
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                let ba = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(match val {
                        Some(vs) => {
                            if vs < 0 {
                                ColumnData::DateTime2(None)
                            } else {
                                to_col_dt((unix_min + StdDuration::from_micros(vs as u64)).to_sql())
                            }
                        }
                        None => ColumnData::DateTime2(None),
                    });
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                let ba = col
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(match val {
                        Some(vs) => {
                            if vs < 0 {
                                ColumnData::DateTime2(None)
                            } else {
                                to_col_dt((unix_min + StdDuration::from_nanos(vs as u64)).to_sql())
                            }
                        }
                        None => ColumnData::DateTime2(None),
                    });
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Null => {
                if coltype == &ColumnType::BigVarChar
                    || coltype == &ColumnType::Text
                    || coltype == &ColumnType::NVarchar
                    || coltype == &ColumnType::NChar
                    || coltype == &ColumnType::BigChar
                    || coltype == &ColumnType::NText
                {
                    for rowindex in 0..rows {
                        token_rows[rowindex].push(ColumnData::String(None));
                    }
                } else if coltype == &ColumnType::Datetime
                    || coltype == &ColumnType::Datetimen
                    || coltype == &ColumnType::Datetime4
                {
                    for rowindex in 0..rows {
                        token_rows[rowindex].push(ColumnData::DateTime(None));
                    }
                } else if coltype == &ColumnType::Bit || coltype == &ColumnType::Bitn {
                    for rowindex in 0..rows {
                        token_rows[rowindex].push(ColumnData::Bit(None));
                    }
                } else {
                    for rowindex in 0..rows {
                        token_rows[rowindex].push(ColumnData::I32(None));
                    }
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
                if coltype == &ColumnType::Int2 {
                    let mut rowindex = 0;
                    for val in ba.iter() {
                        token_rows[rowindex].push(ColumnData::I16(val.map(|v| v as i16)));
                        rowindex += 1;
                    }
                } else {
                    let mut rowindex = 0;
                    for val in ba.iter() {
                        token_rows[rowindex].push(ColumnData::U8(val.map(|v| v as u8)));
                        rowindex += 1;
                    }
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
                    token_rows[rowindex].push(ColumnData::I32(val.map(|x| x.into())));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::UInt32 => {
                let ba = col.as_any().downcast_ref::<UInt32Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(val.map(|x| x.into())));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::UInt64 => {
                let ba = col.as_any().downcast_ref::<UInt64Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(val.map(|x| x as i64)));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Float16 => {
                let ba = col.as_any().downcast_ref::<Float16Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::F32(val.map(|x| x.to_f32())));
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
                if coltype == &ColumnType::Int4 || coltype == &ColumnType::Intn {
                    let mut rowindex = 0;
                    for val in ba.iter() {
                        token_rows[rowindex].push(ColumnData::I32(val.map(|v| v as i32)));
                        rowindex += 1;
                    }
                } else {
                    let mut rowindex = 0;
                    for val in ba.iter() {
                        token_rows[rowindex].push(ColumnData::F64(val));
                        rowindex += 1;
                    }
                }
            }
            arrow::datatypes::DataType::Date32 => {
                let ba = col.as_any().downcast_ref::<Date32Array>().unwrap();
                if coltype == &ColumnType::Datetime || coltype == &ColumnType::Datetimen {
                    let mut rowindex = 0;
                    for val in ba.iter() {
                        token_rows[rowindex].push(match val {
                            Some(vs) => {
                                let days = sql_min_dt_to_unix_min + (vs as i64);
                                ColumnData::DateTime(Some(tiberius::time::DateTime::new(
                                    days.try_into().unwrap(),
                                    0,
                                )))
                            }
                            None => ColumnData::DateTime(None),
                        });
                        rowindex += 1;
                    }
                } else {
                    let mut rowindex = 0;
                    for val in ba.iter() {
                        token_rows[rowindex].push(match val {
                            Some(vs) => {
                                let days = sql_min_to_unix_min + (vs as i64);
                                ColumnData::Date(Some(tiberius::time::Date::new(
                                    days.try_into().unwrap(),
                                )))
                            }
                            None => ColumnData::Date(None),
                        });
                        rowindex += 1;
                    }
                }
            }
            arrow::datatypes::DataType::Date64 => {
                let ba = col.as_any().downcast_ref::<Date64Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(match val {
                        Some(vs) => {
                            let days = sql_min_to_unix_min + vs;
                            ColumnData::Date(Some(tiberius::time::Date::new(
                                days.try_into().unwrap(),
                            )))
                        }
                        None => ColumnData::Date(None),
                    });
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Second) => {
                let ba = col.as_any().downcast_ref::<Time32SecondArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(match val {
                        Some(vs) => {
                            if vs < 0 {
                                ColumnData::Time(None)
                            } else {
                                to_col_dt(
                                    Time::from_hms(
                                        // TODO: Testing
                                        (vs / 60 / 60).try_into().unwrap(),
                                        ((vs / 60) % 60).try_into().unwrap(),
                                        (vs % 60).try_into().unwrap(),
                                    )
                                    .unwrap()
                                    .to_sql(),
                                )
                            }
                        }
                        None => ColumnData::Time(None),
                    });
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                let ba = col.as_any().downcast_ref::<Time32SecondArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(match val {
                        Some(vs) => {
                            if vs < 0 {
                                ColumnData::Time(None)
                            } else {
                                to_col_dt(
                                    Time::from_hms_milli(
                                        (vs / 1000 / 60 / 60).try_into().unwrap(),
                                        ((vs / 1000 / 60) % 60).try_into().unwrap(),
                                        ((vs / 1000) % 60).try_into().unwrap(),
                                        (vs % 1000).try_into().unwrap(),
                                    )
                                    .unwrap()
                                    .to_sql(),
                                )
                            }
                        }

                        None => ColumnData::Time(None),
                    });
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Binary => {
                let ba = col.as_any().downcast_ref::<BinaryArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Binary(val.map(Cow::from)));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::LargeBinary => {
                let ba = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Binary(val.map(Cow::from)));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::FixedSizeBinary(_) => {
                let ba = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::Binary(val.map(Cow::from)));
                    rowindex += 1;
                }
            }
            arrow::datatypes::DataType::Decimal128(_, s) => {
                let ba = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let scale: u8 = (*s).try_into().unwrap();
                let mut rowindex = 0;
                match coltype {
                    ColumnType::Numericn | ColumnType::Decimaln => {
                        for val in ba.iter() {
                            token_rows[rowindex].push(ColumnData::Numeric(
                                val.map(|x| Numeric::new_with_scale(x, scale)),
                            ));
                            rowindex += 1;
                        }
                    }
                    ColumnType::Floatn => {
                        for val in ba.iter() {
                            token_rows[rowindex].push(ColumnData::F64(val.map(|x| {
                                Decimal::from_i128_with_scale(x, scale.into())
                                    .to_f64()
                                    .unwrap()
                            })));
                            rowindex += 1;
                        }
                    }
                    _ => {
                        return Err(LakeApi2SqlError::NotSupported {
                            dtype: col.data_type().clone(),
                            column_type: *coltype,
                        })
                    } //other => panic!("Not supported {:?}", other),
                }
            }
            dt => {
                return Err(LakeApi2SqlError::NotSupported {
                    dtype: dt.clone(),
                    column_type: *coltype,
                })
            } //other => panic!("Not supported {:?}", other),
        }
    }
    Ok(token_rows)
}
