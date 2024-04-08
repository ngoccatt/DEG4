import SideBar from "../../Components/Sidebar"
import Header from "../../Components/Header"
const MainLayout = ({children}) => {
    return(

        <div style={ {backgroundColor: "#F9FAFB", height: "100vh", overflowX: "hidden"}}>
            <SideBar/>
            <Header/>
            <div className="mt-0 p-3 " style={{marginLeft: "15%", height: "92%"}}>
                {children}
            </div>
            </div>
    )
}

export default MainLayout