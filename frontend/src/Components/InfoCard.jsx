import React from "react";
import {IconCoin} from "@tabler/icons-react"

const InfoCard = (props) => {
  return (
    <div
      style={{
        backgroundColor: "#fff",
        borderRadius: "6px",
        width: "300px",
        height: "100px",
        border: "1px solid #eee",
        boxShadow: "-1px 1px 1px 0px rgba(0,0,0,0.25)",
        padding: "12px 8px"
      }}
    >
      <div style={{
        display: 'flex',
        justifyContent:"space-between",
        alignItems: 'center',
        paddingBottom: "12px"

      }}>

        <p style={{
          fontWeight: 800,
          fontSize: "12px",
          color: "#868E96"
        }}>PRICE</p>
        <IconCoin style={{
          width: "16px",
          height: "16px",
          color: "#000"
        }}/>
        </div>
      <p style={{
        fontWeight: 700,
        fontSize: "20px",
        color: "#000",
      }}>12,456</p>
      <p style={{
        color : "#868e96",
        fontSize: "10px"
      }}>Increase compared to last month</p>
    </div>
  );
};

export default InfoCard;
