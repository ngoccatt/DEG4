import { useNavigate } from "react-router-dom";

const Login = () => {
    const navigate = useNavigate();

    const submitHandel = () => {
        navigate('/order')
    }

    return (
        <>
            <div className="container-fluid w-50 ">
                <p className="mb-0">Welcome back</p>
                <h2 >Login to your account</h2>
                <form className="mt-4 mb-5" onSubmit={submitHandel}>
                    <div className="mb-3">
                        <label for="userName" className="form-label">Username</label>
                        <input className="form-control" id="userName" />
                    </div>
                    <div className="mb-3">
                        <label for="passWord" className="form-label">Password</label>
                        <input className="form-control" id="passWord" type="password" />
                    </div>
                    <div className="row mb-3 m-0">
                        <div className="col-md form-check ">
                            <input className="form-check-input" type="checkbox" value="" id="remember" />
                            <label className="form-check-label" for="remember" style={{ fontSize: "12px" }}>
                                Remember me
                            </label>
                        </div>
                    </div>
                    <div className="col-12 mt-4">
                       <button type="submit" className="btn btn-primary w-100">Login now</button> 
                    </div>
                </form>
            </div>

        </>
    )
}

export default Login