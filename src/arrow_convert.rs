use arrow2::array::Array;
use arrow2::array::BooleanArray;
use arrow2::array::Float16Array;
use arrow2::array::Int32Array;
use arrow2::array::NullArray;
use arrow2::array::PrimitiveArray;
use arrow2::array::Utf8Array;
use arrow2::chunk::Chunk;
use std::borrow::Cow;
use std::time::Duration as StdDuration;
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

pub(crate) fn get_token_rows<'a>(batch: &'a Chunk<Box<dyn Array>>) -> Vec<TokenRow<'a>> {
    let unix_min_date =
        Date::from_calendar_date(1970, tiberius::time::time::Month::January, 1).unwrap();
    let unix_min: PrimitiveDateTime = unix_min_date.with_time(Time::from_hms(0, 0, 0).unwrap());

    let cols = batch.columns();
    let rows = batch.len();
    let mut token_rows: Vec<TokenRow> = vec![TokenRow::new(); rows];

    for col in cols {
        //For docs: col.data_type().to_physical_type()
        match col.data_type() {
            arrow2::datatypes::DataType::Boolean => {
                let ba = col.as_any().downcast_ref::<BooleanArray>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(val.into_sql());
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Int32 => {
                let ba = col.as_any().downcast_ref::<Int32Array>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I32(val.copied()));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Utf8 => {
                let ba = col.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::String(match val {
                        Some(vs) => Some(Cow::from(vs)),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::LargeUtf8 => {
                let ba = col.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::String(match val {
                        Some(vs) => Some(Cow::from(vs)),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Timestamp(
                arrow2::datatypes::TimeUnit::Millisecond,
                None,
            ) => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => Some(unix_min + StdDuration::from_millis(*vs as u64)),
                        None => None,
                    };
                    token_rows[rowindex].push(to_datetime(dt_val));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Timestamp(
                arrow2::datatypes::TimeUnit::Microsecond,
                None,
            ) => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => Some(unix_min + StdDuration::from_micros(*vs as u64)),
                        None => None,
                    };
                    token_rows[rowindex].push(to_datetime(dt_val));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Null => {
                for rowindex in 0..rows {
                    token_rows[rowindex].push(ColumnData::I32(None));
                }
            }
            arrow2::datatypes::DataType::UInt8 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::U8(val.copied()));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Int8 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I16(match val {
                        Some(v) => Some(*v as i16),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Int16 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i16>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I16(val.copied()));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Int64 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(val.copied()));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::UInt16 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I32(match val {
                        Some(x) => Some(x.clone().into()),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::UInt32 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(match val {
                        Some(x) => Some(x.clone().into()),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::UInt64 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::I64(match val {
                        Some(x) => Some(x.clone() as i64),
                        None => None,
                    }));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Float16 => {
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
            arrow2::datatypes::DataType::Float32 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::F32(val.copied()));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Float64 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    token_rows[rowindex].push(ColumnData::F64(val.copied()));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Date32 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => Some(unix_min_date + Duration::days(*vs as i64)),
                        None => None,
                    };
                    token_rows[rowindex].push(to_date(dt_val));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Date64 => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val = match val {
                        Some(vs) => Some(unix_min_date + Duration::days(*vs)),
                        None => None,
                    };
                    token_rows[rowindex].push(to_date(dt_val));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Time32(unit) => {
                let ba = col.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

                let mut rowindex = 0;
                for val in ba.iter() {
                    let dt_val: Option<Time> = match val {
                        Some(vs) => match unit {
                            arrow2::datatypes::TimeUnit::Second => Some(
                                Time::from_hms(
                                    // TODO: Testing
                                    (vs / 60 / 60).try_into().unwrap(),
                                    ((vs / 60) % 60).try_into().unwrap(),
                                    (vs % 60).try_into().unwrap(),
                                )
                                .unwrap(),
                            ),
                            arrow2::datatypes::TimeUnit::Millisecond => Some(
                                // TODO: Testing
                                Time::from_hms_milli(
                                    (vs / 1000 / 60 / 60).try_into().unwrap(),
                                    ((vs / 1000 / 60) % 60).try_into().unwrap(),
                                    ((vs / 1000) % 60).try_into().unwrap(),
                                    (vs % 1000).try_into().unwrap(),
                                )
                                .unwrap(),
                            ),
                            _ => panic!("Not supported"),
                        },

                        None => None,
                    };
                    token_rows[rowindex].push(to_time(dt_val));
                    rowindex += 1;
                }
            }
            arrow2::datatypes::DataType::Time64(_) => todo!(),
            arrow2::datatypes::DataType::Duration(_) => todo!(),
            arrow2::datatypes::DataType::Interval(_) => todo!(),
            arrow2::datatypes::DataType::Binary => todo!(),
            arrow2::datatypes::DataType::FixedSizeBinary(_) => todo!(),
            arrow2::datatypes::DataType::LargeBinary => todo!(),
            arrow2::datatypes::DataType::List(_) => todo!(),
            arrow2::datatypes::DataType::FixedSizeList(_, _) => todo!(),
            arrow2::datatypes::DataType::LargeList(_) => todo!(),
            arrow2::datatypes::DataType::Struct(_) => todo!(),
            arrow2::datatypes::DataType::Union(_, _, _) => todo!(),
            arrow2::datatypes::DataType::Map(_, _) => todo!(),
            arrow2::datatypes::DataType::Dictionary(_, _, _) => todo!(),
            arrow2::datatypes::DataType::Decimal(_, _) => todo!(),
            arrow2::datatypes::DataType::Decimal256(_, _) => todo!(),
            arrow2::datatypes::DataType::Extension(_, _, _) => todo!(),
            _ => todo!(), //other => panic!("Not supported {:?}", other),
        }
    }
    token_rows
}
