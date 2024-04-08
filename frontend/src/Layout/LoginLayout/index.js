import LoginImage from "../../Assets/LoginImage.png"


const LoginLayout = ({children}) => {
    return(
        <>
            <div className="container-fluid row m-0 p-0 h-100 overflow-hidden">
                <div className="col-6 d-flex align-items-center justify-content-center">
                    {children}
                </div>
                <div className="col-6 m-0 p-0 bg-success  overflow-hidden" style={{height: "100vh"}}><img className="w-100 h-100" src={LoginImage} /></div>
            </div>
        </>
    )
}

export default LoginLayout