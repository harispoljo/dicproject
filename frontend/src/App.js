import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import axios from "axios";

const api = axios.create({
  baseURL: `http://localhost:8080`
})


class App extends Component {
  constructor(props) {
    super(props);

    this.state = {
     data: []
   }

    this.getData = this.getData.bind(this);

  }

  getData() {
  api.get('/api/data').then(res => {
    console.log("getnode", res.data.rows)
    this.setState({
      data: res.data.rows
    });
  })
}


  componentDidMount() {
    this.getData();
  }

  render() {

    return (
      <div className="App">
        <div className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h2>Welcome to React</h2>
        </div>
        <p className="App-intro">
          To get started, edit <code>src/App.js</code> and save to reload.
        </p>
        {this.state.data.map(item => (<li key={item.country}>{item.country+" "+item.corona_cases}</li>))}
      </div>
    );
  }
}

export default App;
