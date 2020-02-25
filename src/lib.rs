mod codec;
mod de;
mod decode;
mod encode;
mod reader;
mod ser;
mod util;
mod writer;

pub mod schema;
pub mod types;

pub use crate::codec::Codec;
pub use crate::de::from_value;
pub use crate::reader::{from_avro_datum, Reader};
pub use crate::schema::{ParseSchemaError, Schema};
pub use crate::ser::to_value;
pub use crate::types::SchemaResolutionError;
pub use crate::util::{max_allocation_bytes, DecodeError};
pub use crate::writer::{to_avro_datum, ValidationError, Writer};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::Reader;
    use crate::schema::Schema;
    use crate::types::{Record, Value};

    use futures::stream::StreamExt;

    //TODO: move where it fits better
    #[tokio::test]
    async fn test_enum_default() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;
        let reader_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let reader_schema = Schema::parse_str(reader_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::with_schema(&reader_schema, &input[..])
            .await
            .unwrap()
            .into_stream();
        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(1, "spades".to_string())),
            ])
        );
        assert!(reader.next().await.is_none());
    }

    //TODO: move where it fits better
    #[tokio::test]
    async fn test_enum_string_value() {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let schema = Schema::parse_str(raw_schema).unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::with_schema(&schema, &input[..])
            .await
            .unwrap()
            .into_stream();
        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(2, "clubs".to_string())),
            ])
        );
        assert!(reader.next().await.is_none());
    }

    //TODO: move where it fits better
    #[tokio::test]
    async fn test_enum_resolution() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let reader_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "ninja", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let reader_schema = Schema::parse_str(reader_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::with_schema(&reader_schema, &input[..])
            .await
            .unwrap()
            .into_stream();
        assert!(reader.next().await.unwrap().is_err());
        assert!(reader.next().await.is_none());
    }

    //TODO: move where it fits better
    #[tokio::test]
    async fn test_enum_no_reader_schema() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::new(&input[..]).await.unwrap().into_stream();
        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(2, "clubs".to_string())),
            ])
        );
    }
    #[tokio::test]
    async fn test_datetime_value() {
        let writer_raw_schema = r#"{
        "type": "record",
        "name": "dttest",
        "fields": [
            {
                "name": "a",
                "type": {
                    "type": "long",
                    "logicalType": "timestamp-micros"
                }
            }
        ]}"#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        let dt = chrono::NaiveDateTime::from_timestamp(1_000, 995_000_000);
        record.put("a", types::Value::Timestamp(dt));
        writer.append(record).unwrap();
        writer.flush().unwrap();
        let input = writer.into_inner();
        let mut reader = Reader::new(&input[..]).await.unwrap().into_stream();
        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Value::Record(vec![("a".to_string(), Value::Timestamp(dt)),])
        );
    }

    #[tokio::test]
    async fn test_illformed_length() {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;

        let schema = Schema::parse_str(raw_schema).unwrap();

        // Would allocated 18446744073709551605 bytes
        let illformed: &[u8] = &[0x3e, 0x15, 0xff, 0x1f, 0x15, 0xff];

        let value = from_avro_datum(&schema, &mut &illformed[..], None).await;
        assert!(value.is_err());
    }
}
